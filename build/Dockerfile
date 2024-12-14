# Use the pulled Airflow image as the base
FROM apache/airflow:2.10.3

# Install additional Python dependencies
COPY Requirements.txt /tmp/Requirements.txt
RUN pip install --no-cache-dir -r /tmp/Requirements.txt
RUN python -c "import stanza; stanza.download('sv'); nlp = stanza.Pipeline('sv', processors='tokenize,pos,lemma')"


