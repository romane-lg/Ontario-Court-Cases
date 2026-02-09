import pandas as pd
import re
from collections import Counter

# Word lists
CERTAINTY_WORDS = ["must", "clearly", "requires", "therefore", "we hold"]
HEDGING_WORDS = ["may", "might", "arguably", "suggests", "could"]

def analyze_text(text):
    text = str(text).lower()
    words = re.findall(r"\b\w+\b", text)
    total_words = len(words)
    word_counts = Counter(words)
    
    certainty_count = sum(word_counts[w] for w in CERTAINTY_WORDS if w in word_counts)
    hedging_count = sum(word_counts[w] for w in HEDGING_WORDS if w in word_counts)
    
    certainty_per_1000 = certainty_count / total_words * 1000
    hedging_per_1000 = hedging_count / total_words * 1000
    
    return pd.Series({
        "total_words": total_words,
        "certainty_per_1000": round(certainty_per_1000, 2),
        "hedging_per_1000": round(hedging_per_1000, 2),
        "certainty_minus_hedging": round(certainty_per_1000 - hedging_per_1000, 2)
    })

# Load CSV
df = pd.read_csv("courtlistener_cases.csv")

# Apply analysis to opinion text
metrics_df = df["opinion_text_plain"].apply(analyze_text)

# Combine metrics with original data if needed
df_metrics = pd.concat([df, metrics_df], axis=1)

# Quick look at results
print(df_metrics.head())
