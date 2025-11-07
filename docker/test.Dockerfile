FROM astro AS test
# test runner deps (pytest, pytest-cov, etc.)
RUN pip install pytest pytest-cov
COPY tests/ ./tests/
CMD ["pytest", "-q", "tests/test_astro_features.py"]
