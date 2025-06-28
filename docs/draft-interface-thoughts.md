# Some thoughts on a draft specification for a Translator data ingest "parser"

## What is DOGMAN?
I'll use "DOGMAN" as a shorthand to refer to the system that we are building that will live in `NCATSTranslator/translator-ingests`. I'll also use "DOGMAN" as shorthand to refer
to the working group (formally, a subgroup of DINGO) that is charged with building this software.
When you encounter "DOGMAN", hopefully it will always be clear by context whether it is the
software system or the working group that is being referred to.

## In DOGMAN, what is a source-specific "parser"?
For the purpose of this document, a "parser" for the DOGMAN system really means the
software that would be required for ingesting a single knowledge source (say, Drugbank), 
from the script to perform the initial download
of that source's flat-file (or flat-files) distribution (i.e., `drugbank.xml`), to a python function (in some python module like `transform_drugbank.py` or whatever) that
would return python objects representing the nodes and edges of the ingest (that could,
in turn, be serialized to JSON-lines KGX by some software that would be common to the
DOGMAN system. It's important to note that, for some kinds of sources, the "parser" may not actually include any parsing code! But that's OK; just understand that we are using "parser" as a term-of-art to mean everything described above. 

## What new subdirectories of `NCATSTranslator/translator-ingests` are being proposed?

- `parsers/`: would contain one subdirectory for each DOGMAN parser, like `parsers/drugbank`, `parsers/ctd`, etc.
- `docs/`: for design documents, specifications, and other technical documents that need version control with branching (versus the GitHub Wiki, which only displays the `main` branch's Wiki)

## What files must be in a DOGMAN source-specific "parser"?
A parser for source `foo` would include three things, each as a separate file in `parsers/foo`:
1. The "extract" script: An _executable_ file `extract_foo` that would download the `foo` source's flat-file distribution, unpack and uncompress it (if necessary), and transform it into one or more record-oriented files i.e., text files for which each line is a "record" (i.e., into a format like JSON-lines or CSV/TSV). The underscore in the filename is because some authors may wish to implement the extract script as a python module, and it is against best practice to use kebab case (i.e., a hyphen) in a python module filename. But the intent here is to accommodate shell scripts, since for many sources, it may be more expedient to implement the extract script as a bash script rather than as a python module.
2. A python module `transform_foo.py` that would contain a function `next_node` that reads from the file(s) produced by the extract script, and produces a biolink `node` object, and a function `next_edge`, that reads from the files produced by the extract script, and produces a biolink `ege` object.
3. A YAML file `foo.yaml`, that will provide metadata about the ingest (and that will be read and be used by the `transform_foo.py` module to constrain the node and edge property names that it will 


