We use asciidoc and docbook - In order to be able to re-generate the user guide:

	sudo apt-get install asciidoc
	sudo apt-get install docbook
	
In pages folder, checkout this project:

	git clone https://github.com/chluehr/docbook x2doc

Then:

	cd x2doc
	./install.sh
	cd ..

Then you can use the regeneration script:

	./gen_user_guide.sh

If you don't want to re-generate the PDF:

	./gen_user_guide.sh --nopdf


