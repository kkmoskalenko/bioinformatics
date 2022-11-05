import os
import re
from redun import cond, task, File, Scheduler

INPUT_DIR = 'input'
OUTPUT_DIR = 'output'

FASTQC = 'bin/FastQC.app/Contents/MacOS/fastqc'
MINIMAP = 'bin/minimap2'
SAMTOOLS = 'bin/samtools'
FREEBAYES = 'bin/freebayes'


@task()
def fastqc_report(file):
    os.system(f'{FASTQC} -o {OUTPUT_DIR} {file.path}')
    filename = os.path.splitext(file.basename())[0]
    return File(os.path.join(OUTPUT_DIR, f'{filename}_fastqc.html'))


@task()
def minimap_align(reference_file, sequence_file):
    output_path = os.path.join(OUTPUT_DIR, f'aligned{os.extsep}sam')
    os.system(f'{MINIMAP} -a {reference_file.path} {sequence_file.path} > {output_path}')
    return File(output_path)


@task()
def samtools_view(sam_file):
    output_path = os.path.join(OUTPUT_DIR, f'aligned{os.extsep}bam')
    os.system(f'{SAMTOOLS} view -Sb -o {output_path} {sam_file.path}')
    return File(output_path)


@task()
def samtools_flagstat(bam_file):
    args = f'{SAMTOOLS} flagstat {bam_file.path}'
    return os.popen(args).read()


@task()
def samtools_sort(bam_file):
    output_path = os.path.join(OUTPUT_DIR, f'aligned_sorted{os.extsep}bam')
    os.system(f'{SAMTOOLS} sort {bam_file.path} > {output_path}')
    return File(output_path)


@task()
def freebayes(reference_file, bam_sorted_file):
    output_path = os.path.join(OUTPUT_DIR, f'final{os.extsep}vcf')
    os.system(f'{FREEBAYES} -f {reference_file.path} {bam_sorted_file.path} > {output_path}')
    return File(output_path)


@task()
def should_continue(text):
    search = re.search(r'mapped \((\d+\.\d+)%', text)
    if search:
        return float(search.group(1)) >= 90
    else:
        return False


@task()
def continue_execution(aligned_bam):
    print('OK')
    sorted_bam = samtools_sort(aligned_bam)
    return 'Finished', freebayes(File('input/reference.fna'), sorted_bam)


@task()
def end_execution():
    return ['Not OK']


@task()
def main(reference_path=os.path.join(INPUT_DIR, 'reference.fna'),
         sequence_path=os.path.join(INPUT_DIR, 'sequence.fastq.gz')):
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    report = fastqc_report(File(reference_path))

    aligned_sam = minimap_align(File(reference_path), File(sequence_path))
    aligned_bam = samtools_view(aligned_sam)
    flagstat_out = samtools_flagstat(aligned_bam)
    task = cond(should_continue(flagstat_out), continue_execution(aligned_bam), end_execution())

    return report, task


if __name__ == "__main__":
    scheduler = Scheduler()
    result = scheduler.run(main())
    print(result[0])
