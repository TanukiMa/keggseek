# process_stanza.py
import stanza
import warnings
from common_process import run_processor

warnings.filterwarnings("ignore", category=UserWarning, module="torch._weights_only_unpickler")

# --- Globals for worker processes ---
_NLP_MODEL = None

def init_stanza():
    """Initializes the Stanza model in a worker process."""
    global _NLP_MODEL
    if _NLP_MODEL is None:
        _NLP_MODEL = stanza.Pipeline('ja', verbose=False, use_gpu=False)

def analyze_with_stanza(text: str) -> list:
    """Uses Stanza to extract entities and nouns."""
    if not text.strip() or _NLP_MODEL is None: return []
    chunk_size = 50000 
    found_words, found_texts = [], set()
    try:
        for i in range(0, len(text), chunk_size):
            chunk = text[i:i + chunk_size]
            doc = _NLP_MODEL(chunk)
            date_texts = {ent.text.strip() for ent in doc.ents if ent.type == 'DATE'}
            for ent in doc.ents:
                if ent.type != 'DATE':
                    word_text = ent.text.strip()
                    if len(word_text) > 1 and word_text not in found_texts:
                        found_words.append({"word": word_text, "source_tool": "stanza", "entity_category": ent.type, "pos_tag": "ENT"})
                        found_texts.add(word_text)
            for sentence in doc.sentences:
                for word in sentence.words:
                    word_text = word.text.strip()
                    if word.upos == "NOUN" and len(word_text) > 1 and word_text not in found_texts and word_text not in date_texts:
                        found_words.append({"word": word_text, "source_tool": "stanza", "entity_category": "NOUN_GENERAL", "pos_tag": word.xpos})
                        found_texts.add(word_text)
    except Exception as e:
        print(f"  [!] Stanza analysis error: {e}", file=sys.stderr)
    return found_words

if __name__ == "__main__":
    run_processor(tool_name="stanza", analyze_fn=analyze_with_stanza, init_fn=init_stanza)