# Colossus Documentation Site

### Branches

* `gh-pages` - contains only the compiled site
* `gh-pages-source` - contains only the source files

### Editing the docs

You must have installed on your computer:
 * ruby 2.0 or greater
 * jekyll
 * sass (gem install sass)
 * rake

To start a localhost server, simply go to the root folder of this repo and run :

`jekyll serve --watch --baseurl=''`

the browse to localhost:4000

### publishing

When publishing just run :

`rake site:publish`

This will build the site, merge it into the existing `gh-pages` branch, and push the commit



