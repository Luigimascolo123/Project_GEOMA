import spacy
import re
from urllib.parse import urlparse, unquote
from typing import List
import logging

class KeywordExtractor:
    # Inizializzazione modello spaCy
    def __init__(self):
        try:
            self.nlp = spacy.load('en_core_web_sm')
        except OSError:
            print("Modello spaCy non trovato. Installarlo con: python -m spacy download en_core_web_sm")
            raise
        
        self.stop_words = self.nlp.Defaults.stop_words
        self.logger = logging.getLogger(__name__)

    def _extract_title_segment(self, url_path: str) -> str:
        """
        Estrae il segmento contenente il titolo dall'URL usando analisi linguistica
        """
        try:
            decoded_path = unquote(url_path)
            segments = [s for s in decoded_path.strip('/').split('/') if s]
            
            if not segments:
                return ''

            # Cerca il segmento del titolo
            title_segment = ''
            max_score = 0

            for segment in segments:
                # Prima controlla se il segmento ha una struttura che suggerirebbe un ID o un percorso tecnico
                if any([
                    re.match(r'^[\da-f]{8}(-[\da-f]{4}){3}-[\da-f]{12}', segment),  # UUID
                    re.match(r'^[\da-f]{32}', segment),                              # MD5/hash
                    re.match(r'^\d{4}/\d{2}/\d{2}', segment),                       # Data
                    segment.startswith(('api', 'v1', 'v2')),                         # API paths
                    re.match(r'^\d+$', segment)                                      # Solo numeri
                ]):
                    continue

                # Pulisci il segmento mantenendo le parole significative
                cleaned_segment = re.sub(r'[-_]+', ' ', segment)
                cleaned_segment = re.sub(r'\.\w+$', '', cleaned_segment)  # Rimuovi estensioni file
                cleaned_segment = cleaned_segment.strip()

                # Se dopo la pulizia il segmento è troppo corto, salta 
                # (il segmento deve essere almeno di 5 caratteri)
                if len(cleaned_segment) < 5:  
                    continue

                doc = self.nlp(cleaned_segment)
                
                # Calcola la densità di contenuto significativo
                total_tokens = len([token for token in doc if not token.is_space])
                if total_tokens == 0:
                    continue

                meaningful_tokens = [
                    token for token in doc 
                    if (token.pos_ in ['NOUN', 'PROPN', 'VERB'] and       # Parti del discorso significative
                        len(token.text) > 2 and                           # Lunghezza minima
                        not token.like_num and                            # Non numerico
                        not token.is_punct and                            # Non punteggiatura
                        not token.is_space)                               # Non spazi
                ]

                # Calcola score basato su:
                # 1. Rapporto tra token significativi e totali
                # 2. Presenza di una struttura frasale (soggetto/verbo/complemento)
                if meaningful_tokens:
                    content_density = len(meaningful_tokens) / total_tokens
                    has_noun = any(t.pos_ in ['NOUN', 'PROPN'] for t in meaningful_tokens)
                    has_verb = any(t.pos_ == 'VERB' for t in meaningful_tokens)
                    
                    score = (
                        content_density * 10 +              # Densità contenuto
                        len(meaningful_tokens) * 2 +        # Numero token significativi
                        (has_noun and has_verb) * 5        # Struttura frasale
                    )

                    if score > max_score:
                        max_score = score
                        title_segment = cleaned_segment

            return title_segment.strip()

        except Exception as e:
            self.logger.warning(f"Error extracting title from URL path: {str(e)}")
            return ''
        
    def extract_keywords(self, url: str, max_keywords: int = 10) -> List[str]:
        """
        Estrae keywords dall'URL
        """
        if not url or not isinstance(url, str):
            return []
        
        try:
            parsed_url = urlparse(url)
            title_segment = self._extract_title_segment(parsed_url.path)
            
            if not title_segment:
                return []
            
            doc = self.nlp(title_segment)
            keywords = []
            
            # Dizionario per tenere traccia delle radici comuni
            lemma_dict = {} #
            
            for token in doc:
                is_acronym = token.text.isupper() and len(token.text) >= 2
                
                if (
                    is_acronym or
                    (
                        token.pos_ in ['NOUN', 'PROPN', 'ADJ', 'VERB'] and
                        len(token.text) >= 2 and
                        not token.is_stop and
                        not token.like_num and
                        not token.is_punct and
                        not token.is_space
                    )
                ):
                    if is_acronym:
                        keyword = token.text
                    else:
                        # Usa il lemma per le altre parole
                        keyword = token.lemma_
                    
                    # Aggiungi alla lista solo se non abbiamo già una parola con la stessa radice
                    if keyword.lower() not in lemma_dict:
                        lemma_dict[keyword.lower()] = True
                        keywords.append(keyword.lower())
            
            return list(dict.fromkeys(keywords))[:max_keywords]
            
        except Exception as e:
            self.logger.warning(f"Error extracting keywords from URL {url}: {str(e)}")
            return []