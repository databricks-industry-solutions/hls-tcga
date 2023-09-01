# The Cancer Genome Atlas (TCGA)

<img src="https://www.cancer.gov/ccg/sites/g/files/xnrzdm256/files/styles/cgov_featured/public/cgov_image/media_image/2022-06/TCGA%20people%20and%20layers%20of%20data%20425x319.jpg?h=982f41e1&itok=zkQ_l8-t" width=200 >
 
The Cancer Genome Atlas (TCGA) is a comprehensive and coordinated effort to accelerate our understanding of the molecular basis of cancer through the application of genome analysis technologies, including large-scale genome sequencing. TCGA was launched in 2006 by the National Cancer Institute (NCI) and National Human Genome Research Institute (NHGRI).

The goals of TCGA are:
1. Improve our ability to diagnose, treat and prevent cancer. By better understanding the genomic changes involved in cancer, we can develop more effective diagnostic and treatment approaches.
2. Identify molecular targets for therapy. By finding shared molecular characteristics of tumors, we can develop targeted therapies that attack those specific targets.
3. Reveal mechanisms of carcinogenesis. Identifying the genomic changes involved in the development of cancer sheds light on mechanisms by which normal cells become tumors. 
4. Improve cancer recurrence prediction. Learning about the genomic changes in tumors can help identify markers that indicate a higher risk of cancer coming back after treatment. 
5. Enable new discoveries through data sharing. TCGA shares all its data and analyses with the entire scientific community so anyone can conduct their own research, make new discoveries and develop better solutions.

TCGA currently has over 2.5 petabytes of genomic, epigenomic, transcriptomic, and proteomic data from 33 types of cancer. Over 10,000 patients have contributed tumor samples and matched controls from blood or adjacent normal tissues. All data is available through the Genomic Data Commons, visually exploreable via the Integrated Genomics Viewer.

TCGA is an unprecedented comprehensive catalog of the key genomic changes in major cancers. It continues to drive major advances in understanding cancer biology, revealing insights into mechanisms of tumorigenesis, and paving the way for new diagnostic and therapeutic approaches.

## Workflow overview
<img src="https://hls-eng-data-public.s3.amazonaws.com/img/tcga-umap.png" width=500 >

In this demo, we show how easily you can load RNA epxression profiles from [TCGA](https://portal.gdc.cancer.gov/) and clincial data associated with the samples into databricks lakehouse platform and apply different analysis on the dataset. 

- First we land RNA expession profiles along with clinical metadatae using <a href="$./00-data-download">00-data-download</a>
notebook which uses [GDC APIs](https://gdc.cancer.gov/access-data/gdc-data-transfer-tool) to collect associated metadata and land data in the cloude strorage.

- Next we in the <a href="$./01-tcga-etl">01-tcga-etl</a> notebook we create tables and publish these tables to Unity Catalog. 
Alternatively you can use <a href="$./01-tcga-dlt">01-tcga-dlt</a> to perform the same tasks using DLT pipelines. 

- In <a href="$./02-rna-tcga-analysis"> 02-rna-tcga-analysis </a> we show examples of exploring the data using `sql` and `pyspark-ai` for interacting with the tables in natural language. 

- In the next step, in <a href="$03-rna-tcga-expression-profiles"> 03-rna-tcga-expression-profiles</a> we create a dataset of normalized gene expressions for each sample and select the most variable featueres and apply UMAP dimensionality reduction on these features for data visualization and create an interactive dashboard for exploratroty analysis of the RNA clsuters.

[![](https://mermaid.ink/img/pako:eNptkU1qwzAQha8itOgquYAWAWOXUmhKaNqdIUytsSvQjyuNFiHk7h3ZzsK4Wo3e-2b0xNxkFzRKJRP-ZvQdNgaGCK71gs8IkUxnRvAkXpr6Up1ek4BUalHqLVXbkLU4U4gw4NZu3j634pc3dL3UQGDDUMZPgliEf_hjdWr9rD9S7Q-H1dNKfLxXYoyhNxbTk0MCzfPmphXJnZxKCY2WQBB8Mz9jLLO5SqfWqVZeYTkZTzJORNS5IxOWmMXYFyBhnKN1NifCyE_JnXQYHRjNa7gVvpX0gw5bqbjU2EO21MrW3xnNI_8Dn7Xh9FJRzLiTkCmcr7573Gdm2aRUPdjEKk49x3nd09bvfwl_q6E?type=png)](https://mermaid.live/edit#pako:eNptkU1qwzAQha8itOgquYAWAWOXUmhKaNqdIUytsSvQjyuNFiHk7h3ZzsK4Wo3e-2b0xNxkFzRKJRP-ZvQdNgaGCK71gs8IkUxnRvAkXpr6Up1ek4BUalHqLVXbkLU4U4gw4NZu3j634pc3dL3UQGDDUMZPgliEf_hjdWr9rD9S7Q-H1dNKfLxXYoyhNxbTk0MCzfPmphXJnZxKCY2WQBB8Mz9jLLO5SqfWqVZeYTkZTzJORNS5IxOWmMXYFyBhnKN1NifCyE_JnXQYHRjNa7gVvpX0gw5bqbjU2EO21MrW3xnNI_8Dn7Xh9FJRzLiTkCmcr7573Gdm2aRUPdjEKk49x3nd09bvfwl_q6E)


## TCGA Data Model
[![](https://mermaid.ink/img/pako:eNp90c2OgyAQB_BXIXNuX8AbK8RlI63x42ZiWKWtiUKDeNho331BbdOYzXKCmd_8D8wEtW4kBCANacXViL5UyJ0kPUcp5miej8d58s8vGuYoQCXcxFDCSy1lr_SEQpzRhdRaWdGql1saGyKU--jkk4X7uHeWYZ7EdC-26mZSikkVpeci2bu3zjMvZoRWjONoDf2WnVbXobL6n5nig7M8p6QqTjhm0cndvMr-SIAD9NL0om3cZ04-sQR7k70sweNGXsTYWS8fjo73RlhJm9ZqA4E1ozyAGK3OflT9fK9m2woEF9ENriqXGb4ubdnd4xfYSYp-?type=png)](https://mermaid.live/edit#pako:eNp90c2OgyAQB_BXIXNuX8AbK8RlI63x42ZiWKWtiUKDeNho331BbdOYzXKCmd_8D8wEtW4kBCANacXViL5UyJ0kPUcp5miej8d58s8vGuYoQCXcxFDCSy1lr_SEQpzRhdRaWdGql1saGyKU--jkk4X7uHeWYZ7EdC-26mZSikkVpeci2bu3zjMvZoRWjONoDf2WnVbXobL6n5nig7M8p6QqTjhm0cndvMr-SIAD9NL0om3cZ04-sQR7k70sweNGXsTYWS8fjo73RlhJm9ZqA4E1ozyAGK3OflT9fK9m2woEF9ENriqXGb4ubdnd4xfYSYp-)
