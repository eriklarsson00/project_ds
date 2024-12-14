import stanza

nlp = stanza.Pipeline(lang='fr', processors='tokenize,mwt')
doc = nlp('Nous avons atteint la fin du sentier.')
for token in doc.sentences[0].tokens:
    print(f'token: {token.text}\twords: {", ".join([word.text for word in token.words])}')
