#!/bin/bash

#
# A script for auto-generating the full single-page HTML of the user_guide.txt with asciidoc + bootstrap + github pages
#

# Call asciidoc
asciidoc -a toc -a linkcss -a stylesheet=css/bootstrap.min.css --out-file user_guide.html user_guide.txt

# Get rid of all the header until <body> part
NLINES=`wc -l user_guide.html | awk '{ print $1 }'`
GETLINES=$(expr $NLINES - 18)
TAILLINES=$(expr $GETLINES - 2)

# Get rid also of the </body></html>
tail "-$GETLINES" user_guide.html | head "-$TAILLINES" > user_guide_trimmed.html

# Add the github pages template thingy
echo "---" > user_guide.html
echo "layout: default" >> user_guide.html
echo "title: Splout SQL User Guide" >> user_guide.html
echo "---" >> user_guide.html

echo "<script>window.onload = function(){asciidoc.footnotes(); asciidoc.toc(2);}</script>" >> user_guide.html

# Mix it up
cat user_guide_trimmed.html >> user_guide.html

# And get rid of the temporal file
rm user_guide_trimmed.html
