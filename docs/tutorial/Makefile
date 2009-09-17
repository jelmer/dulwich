TXT=$(shell ls *.txt)

ALL: index.html

index.html: $(TXT)
	rst2html.py index.txt index.html

clean:
	rm -f index.html

.PHONY: clean
