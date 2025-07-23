rule all:
    input:
        "sync.txt"

rule download:
    input:
        "startfile.txt"
    output:
        "datafile.txt"
    shell:
        "echo '##### 01928374756' > {output}"


rule find_version:
    input:
        "datafile.txt"
    output:
        "versionfile.txt"
    shell:
        "echo '90' > {output}"


checkpoint version_files:
    input:
        "versionfile.txt"
    output:
        touch("/tmp/versionfile.tmp")

# Used https://raw.githubusercontent.com/ctb/2021-snakemake-checkpoints-example/refs/heads/latest/Snakefile.count as an example
class Checkpoint_GenerateVersionedFileList():
    def get_version(self):
        with open('versionfile.txt', 'r') as file:
            for line in file:
                return line.strip()

    def __call__(self, w):
        global checkpoints

        checkpoints.version_files.get(**w)

        version = self.get_version()

        versioned_datafile = "datafile-" + version + ".txt"
        versioned_outputfile = "outputfile-" + version + ".txt"
        versioned_normalizedfile = "normalizedfile-" + version + ".txt"

        return [versioned_datafile, versioned_outputfile, versioned_normalizedfile]


rule version_rename:
    input:
        "datafile.txt"
    output:
        "datafile-{ver}.txt"
    shell:
        "mv {input} {output}"

rule output:
    input:
        "datafile-{ver}.txt"
    output:
        "outputfile-{ver}.txt"
    shell:
        "echo '11 22 33 44 55 66 77 88 99 00' > {output}"

rule normalize:
    input:
        "outputfile-{ver}.txt"
    output:
        "normalizedfile-{ver}.txt"
    shell:
        "echo '99 00 77 88 55 66 33 44 11 22' > {output}"


rule finalize:
    input:
        Checkpoint_GenerateVersionedFileList()
    output:
        "sync.txt"
    shell:
        "echo '1' > {output}"

