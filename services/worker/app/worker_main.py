from jobs.score_traits import score_vectors_bio

def test_local_llm():
    text = "Albert Einstein was a theoretical physicist recognised for the theory of relativity..."
    print(score_vectors_bio(text))

if __name__ == "__main__":
    test_local_llm()
