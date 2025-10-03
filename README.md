# Data Intelligence for R&D: The Cancer Genome Atlas (TCGA)

<img src="https://www.cancer.gov/ccg/sites/g/files/xnrzdm256/files/styles/cgov_featured/public/cgov_image/media_image/2022-06/TCGA%20people%20and%20layers%20of%20data%20425x319.jpg?h=982f41e1&itok=zkQ_l8-t" width=500 >


[The Cancer Genome Atlas (TCGA)](https://www.cancer.gov/ccg/research/genome-sequencing/tcga) represents a comprehensive and coordinated initiative aimed at expediting our understanding of the molecular foundations of cancer by leveraging genome analysis technologies, including large-scale genome sequencing. Spearheaded in 2006 by the [National Cancer Institute (NCI)](https://www.nih.gov/about-nih/what-we-do/nih-almanac/national-cancer-institute-nci) and [National Human Genome Research Institute (NHGRI)](https://www.genome.gov/), TCGA has set forth the following objectives:

1. Enhance our capacity for cancer diagnosis, treatment, and prevention by delving into the genomic alterations in cancer, which will pave the way for more refined diagnostic and therapeutic strategies.
2. Pinpoint molecular therapy targets by discerning common molecular traits of tumors, enabling the development of treatments that specifically target these markers.
3. Uncover carcinogenesis mechanisms by identifying the genomic shifts that lead to the transition of normal cells into tumors.
4. Strengthen predictions of cancer recurrence by understanding the genomic modifications in tumors, facilitating the recognition of indicators that signify an increased likelihood of cancer resurgence post-treatment.
5. Foster new breakthroughs via data sharing. TCGA has adopted a policy of sharing all its data and findings with the global scientific fraternity, promoting independent research, novel discoveries, and the development of improved solutions.

TCGA boasts over 2.5 petabytes of genomic, epigenomic, transcriptomic, and proteomic data spanning 33 cancer types. Contributions from over 10,000 patients include tumor samples and matched controls from blood or nearby normal tissues. The Genomic Data Commons offers complete access to this data, and users can visually navigate it using the Integrated Genomics Viewer.

TCGA serves as a comprehensive repository of pivotal genomic variations in major cancers, continually propelling significant advancements in cancer biology comprehension. It illuminates the mechanisms underlying tumorigenesis and sets the stage for the next generation of diagnostic and therapeutic methods.

## Workflow Overview
Within this solution accelerator, we present a template illustrating the ease with which one can load RNA expression profiles from [TCGA](https://portal.gdc.cancer.gov/) and associated clinical data into the Databricks platform, and subsequently perform diverse analyses on the dataset. Specifically, we demonstrate how to construct a database of gene expression profiles combined with pertinent metadata and manage all data assets, including raw files, using [Unity Catalog (UC)](https://www.databricks.com/product/unity-catalog). Below is an outline of the workflow:

- Modify [config.json](./config.json) file if needed to customize your catalog
- Run [setup](./00-setup) notebook to create the catalog, schema and associated volume to store raw data
- Initially, RNA expression profiles and clinical metadata can be downloaded using the [01-data-download](./01-data-download) notebook. This notebook uses [GDC APIs](https://gdc.cancer.gov/access-data/gdc-data-transfer-tool) to download and store the data into a [managed volume](https://docs.databricks.com/en/data-governance/unity-catalog/create-volumes.html), a Unity Catalog-governed storage volume housed in the schema's default storage location.

- Subsequently, in the [etl_pipelines](./etl_pipelines), we create tables from raw data using [Lakeflow ETL](https://www.databricks.com/product/data-engineering/lakeflow-declarative-pipelines) pipelines.

- As an example of using databricks data intelligence [02-tcga-expression-clsutering](./02-tcga-expression-clsutering), we use dimensionality reduction and clustering to investigate relationships between RNA expression profiles and meta data such as tissue or oragn of origin. Most of this notebook is created by using [Databricks Data Science Agent](https://www.databricks.com/blog/introducing-databricks-assistant-data-science-agent). 

### Workflow

#### Data Download
We use the following enedpoints to download open access data:

cases_endpt: https://api.gdc.cancer.gov/cases

files_endpt: https://api.gdc.cancer.gov/files

data_endpt:  https://api.gdc.cancer.gov/data

#### ETL
After landing the files in a managed volume, we transform the data into the following tables:

[![](https://mermaid.ink/img/pako:eNqFk1Fr2zAQx7-K0Ah-SUe2tWz1wyB1lFHoYCxbKdjDKNY5FdiSkeTRUvLde7YjW04f6gdb-v9_p9PJuhdaaAE0povFS6YIkUq6mPRDQiL3CDVEMYn23EK0DNV7biTfV2CjEUerMbLm5jnRlTZd3IfLz9fXbOVDJ-IPPLmJKsvyLXKjjQAzQV-TFT4BV0kFk726_HJ1tQlsC4VWYrabb-tkvd0GjAPj5Ay5WbNP2yQaiGP3wddxscjUwfDmkdz9Hizb7gfhb0LOlHtdtTXYQU3SAs_OklLiWf0bNJbCU2PAWqnVYJAaHBfccU_8CpHG6DAclBgGZ3l3Bf4Y7vOeEvsFw5x5v1ouValHO0yYnyXc3KZC8oPSwXoPHa9tayaKpQJq3e9FFl79cbdLD6Agr-A_VHmQxDruPLVDyvK6qd7hxtL7walScnHxHes9baybMeaPsfewuhFG-iNKm1tf-HzuffYw9_18jGdn8cxnYF1SlLDwuYA10iWtwdRcCuy4vmsy2ndTRmMcCih5W7mM4o1DtG3wRgAT0mlDY2daWFLeOr17VgWNS15Z8NAGf4_h9ahCH_RzaO2-w4-vPW4yHw?type=png)](https://mermaid.live/edit#pako:eNqFk1Fr2zAQx7-K0Ah-SUe2tWz1wyB1lFHoYCxbKdjDKNY5FdiSkeTRUvLde7YjW04f6gdb-v9_p9PJuhdaaAE0povFS6YIkUq6mPRDQiL3CDVEMYn23EK0DNV7biTfV2CjEUerMbLm5jnRlTZd3IfLz9fXbOVDJ-IPPLmJKsvyLXKjjQAzQV-TFT4BV0kFk726_HJ1tQlsC4VWYrabb-tkvd0GjAPj5Ay5WbNP2yQaiGP3wddxscjUwfDmkdz9Hizb7gfhb0LOlHtdtTXYQU3SAs_OklLiWf0bNJbCU2PAWqnVYJAaHBfccU_8CpHG6DAclBgGZ3l3Bf4Y7vOeEvsFw5x5v1ouValHO0yYnyXc3KZC8oPSwXoPHa9tayaKpQJq3e9FFl79cbdLD6Agr-A_VHmQxDruPLVDyvK6qd7hxtL7walScnHxHes9baybMeaPsfewuhFG-iNKm1tf-HzuffYw9_18jGdn8cxnYF1SlLDwuYA10iWtwdRcCuy4vmsy2ndTRmMcCih5W7mM4o1DtG3wRgAT0mlDY2daWFLeOr17VgWNS15Z8NAGf4_h9ahCH_RzaO2-w4-vPW4yHw)
