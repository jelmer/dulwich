RST2HTML = rst2html
TXT=$(shell ls *.txt)

ALL: index.html

index.html: $(TXT)
	$(RST2HTML) index.txt index.html

clean:
	rm -f index.html

.PHONY: clean
