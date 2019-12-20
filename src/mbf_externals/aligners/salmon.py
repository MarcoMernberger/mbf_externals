from ..externals import ExternalAlgorithm
import pypipegraph as ppg
from pathlib import Path
from ..util import download_file
import hashlib


class Salmon(ExternalAlgorithm):
    def __init__(self, accepted_biotypes, version="_last_used", store=None):
        """@accepted_biotypes may be a set, or None to use all"""
        if accepted_biotypes is not None and not isinstance(accepted_biotypes, set):
            raise ValueError("@accepted_biotypes may be a set, or None to use all")
        self.accepted_biotypes = accepted_biotypes
        super().__init__(version, store)

    latest_version = "1.0.0"

    @property
    def name(self):
        return "Salmon"

    @property
    def multi_core(self):
        return True

    def build_cmd(self, output_directory, ncores, arguments):
        return (
            [self.path / "salmon-latest_linux_x86_64" / "bin" / "salmon"]
            + arguments
            + ["-p", str(ncores)]
        )

    # def _aligner_build_cmd(self, output_dir, ncores, arguments):
    # return arguments + ["--runThreadN", str(ncores)]

    def get_index_version_range(self):  # pragma: no cover
        return None, None

    def get_genome_deps(self, genome):
        return [genome.job_transcripts()]

    def get_build_key(self):
        return hashlib.md5(
            str(sorted(self.accepted_biotypes)).encode("utf-8")
        ).hexdigest()

    def build_index_from_genome(self, genome, output_fileprefix):
        import pysam

        output_dir = Path(output_fileprefix)
        output_key = self.get_build_key()
        output_dir.mkdir(parents=True, exist_ok=True)
        temp_dir = Path("cache") / "salmon" / genome.name / output_key
        temp_dir.mkdir(parents=True, exist_ok=True)

        try:
            tf_cdna = open(temp_dir / "transcripts.fasta", "w")
            of_mapping = open(output_dir / "gene_transcript.mapping", "w")

            to_output = set()
            genes_out = set()

            for transcript in genome.transcripts.values():
                if (
                    self.accepted_biotypes is None
                    or transcript.biotype in self.accepted_biotypes
                ):
                    to_output.add(transcript.transcript_stable_id)
                    genes_out.add(transcript.gene_stable_id)
                    of_mapping.write(
                        f"{transcript.transcript_stable_id}\t{transcript.gene_stable_id}\n"
                    )

            seen = set()
            # iterating the fasta once is much faster than random accessing for each and every transcript
            with pysam.FastxFile(genome.find_file("cdna.fasta")) as f:
                for entry in f:
                    name = entry.name
                    if not name in to_output and "." in name:
                        name = name[: name.rfind(".")]
                    if name in to_output:
                        tf_cdna.write(f">{name}\n{entry.sequence}\n")
                        seen.add(name)
            if not seen:
                raise ValueError("non seeen", entry.name)
            for transcript_stable_id in to_output.difference(seen):
                mrna = genome.transcripts[transcript_stable_id].mrna
                tf_cdna.write(f">{transcript_stable_id}\n{mrna}\n")

            with open(output_fileprefix / "mt.genes", "w") as op:
                for gene_stable_id in genome.df_genes.index[
                    genome.df_genes["name"].str.startswith("MT-")
                ]:
                    if gene_stable_id in genes_out:
                        op.write(gene_stable_id + "\n")
            with open(output_fileprefix / "rrna.genes", "w") as op:
                for gene_stable_id in genome.df_genes.index[
                    genome.df_genes["biotype"] == "rRNA"
                ]:
                    if gene_stable_id in genes_out:
                        op.write(gene_stable_id + "\n")

            tf_cdna.flush()
            of_mapping.close()
            run_func = self.get_run_func(
                output_fileprefix,
                [
                    "index",
                    "-i",
                    str((output_dir / "index").absolute()),
                    "-k",
                    "31",
                    "--transcripts",
                    str(Path(tf_cdna.name).absolute()),
                ],
            )
            run_func()

        finally:
            tf_cdna.close()
            # os.unlink(tf_cdna.name)
            of_mapping.close()

    def get_latest_version(self):
        return self.latest_version

    def fetch_version(self, version, target_filename):
        url = f"https://github.com/COMBINE-lab/salmon/releases/download/v{version}/salmon-{version}_linux_x86_64.tar.gz"
        # we want a tar.gz, we get a tar.gz
        with open(target_filename, "wb") as op:
            download_file(url, op)

    def run_alevin(
        self,
        output_path,
        fastqs_r1_r2,
        genome,
        method,
        dumpMtx=False,
        dumpFeatures=True,
    ):
        allowed_methods = (
            "chromium",
            "chromuimV3",
            "dropseq",
            "gemcode",
            "celseq",
            "celseq2",
            "quartzseq2",
        )
        if not method in allowed_methods:
            raise ValueError("method must be one of {allowed_methods}")
        output_path = Path(output_path)
        index_path = genome.build_index(self).output_path
        r1s = []
        r2s = []
        try:
            for r1, r2 in fastqs_r1_r2:
                r1s.append(str(r1))
                r2s.append(str(r2))
                if " " in str(r1) or " " in str(r2):
                    raise ValueError("Spaces in fastq names not allowed", r1, r2)
        except ValueError as e:
            if "not enough values" in str(e):
                raise ValueError(
                    "alevin currently works on paired end data like drop seq only?"
                )
            else:
                raise

        cmd = (
            [
                "alevin",
                "-lISR",  # inward, stranded, came from reverse strand... thats' right for dropseq
                "-1",
            ]
            + r1s
            + ["-2"]
            + r2s
            + [
                "--" + method,
                "-i",
                str(index_path / "index"),
                "-o",
                str(Path(output_path).absolute()),
                "--tgMap",
                str(index_path / "gene_transcript.mapping"),
                "--mrna",
                str(index_path / "mt.genes"),
                "--rrna",
                str(index_path / "rrna.genes"),
                "--maxNumBarcodes",
                "100000",  # just so you know what to patch
            ]
        )
        if dumpMtx:
            cmd.append("--dumpMtx")
        if dumpFeatures:
            cmd.append("--dumpFeatures")

        self.get_run_func(output_path, cmd)()

    def run_alevin_on_sample(self, lane, genome, method):
        output = Path("results/alevin/") / lane.name

        def run_alevin():
            output.mkdir(exist_ok=True, parents=True)
            self.run_alevin(
                output, [lane.get_aligner_input_filenames()], genome, method
            )
            (output / "sentinel.txt").write_text("done")

        job = ppg.FileGeneratingJob(output / "sentinel.txt", run_alevin).depends_on(
            genome.build_index(self), lane.prepare_input()
        )

        def run_qc():
            (output / "QC").mkdir(exist_ok=True)
            import rpy2.robjects as ro

            ro.r("library('alevinQC')")
            ro.r("alevinQCReport")(
                baseDir=str(output.absolute()),
                sampleId=lane.name,
                outputFile="alevinReport.html",
                outputFormat="html_document",
                outputDir=str((output / "QC").absolute()),
                forceOverwrite=True,
            )

        qc_job = ppg.FileGeneratingJob(
            output / "QC" / "alevinReport.html", run_qc
        ).depends_on(job)
        return job, qc_job
