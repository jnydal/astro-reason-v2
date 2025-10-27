from jobs.score_traits import score_traits_bio

def test_local_llm():
    text = "Albert Einstein was a theoretical physicist recognised for the theory of relativity..."
    print(score_traits_bio(text))

if __name__ == "__main__":
    test_local_llm()
