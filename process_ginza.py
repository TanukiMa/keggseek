# process_ginza.py
import spacy
import warnings
from common_process import run_processor

warnings.filterwarnings("ignore", category=FutureWarning, module="huggingface_hub")

# --- Globals for worker processes ---
_NLP_MODEL = None

def init_ginza():
    """Initializes the GiNZA model in a worker process."""
    global _NLP_MODEL
    if _NLP_MODEL is None:
        _NLP_MODEL = spacy.load("ja_ginza_electra")

def analyze_with_ginza(text: str) -> list:
    """Uses GiNZA to extract entities and nouns."""
    if not text.strip() or _NLP_MODEL is None: return []
    chunk_size = 40000 
    found_words, found_texts = [], set()
    try:
        for i in range(0, len(text), chunk_size):
            chunk = text[i:i + chunk_size]
            doc = _NLP_MODEL(chunk)
            for ent in doc.ents:
                if ent.label_ != 'DATE':
                    word_text = ent.text.strip()
                    if len(word_text) > 1 and word_text not in found_texts:
                        found_words.append({"word": word_text, "source_tool": "ginza", "entity_category": ent.label_, "pos_tag": "ENT"})
                        found_texts.add(word_text)
            for token in doc:
                word_text = token.text.strip()
                if token.pos_ == "NOUN" and token.ent_type_ == "" and len(word_text) > 1 and word_text not in found_texts:
                     found_words.append({"word": word_text, "source_tool": "ginza", "entity_category": "NOUN_GENERAL", "pos_tag": token.tag_})
                     found_texts.add(word_text)
    except Exception as e:
        print(f"  [!] GiNZA analysis error: {e}", file=sys.stderr)
    return found_words

if __name__ == "__main__":
    run_processor(tool_name="ginza", analyze_fn=analyze_with_ginza, init_fn=init_ginza)