# R&D Lakehouse: The Cancer Genome Atlas (TCGA)

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
Within this solution accelerator, we present a template illustrating the ease with which one can load RNA expression profiles from [TCGA](https://portal.gdc.cancer.gov/) and associated clinical data into the Databricks lakehouse platform, and subsequently perform diverse analyses on the dataset. Specifically, we demonstrate how to construct a database of gene expression profiles combined with pertinent metadata and manage all data assets, including raw files, using [Unity Catalog (UC)](https://www.databricks.com/product/unity-catalog). Below is an outline of the workflow:

- Initially, RNA expression profiles and clinical metadata are downloaded using the [00-data-download](./00-data-download) notebook. This action leverages [GDC APIs](https://gdc.cancer.gov/access-data/gdc-data-transfer-tool) to store the data into a [managed volume](https://docs.databricks.com/en/data-governance/unity-catalog/create-volumes.html), a Unity Catalog-governed storage volume housed in the schema's default storage location.

- Subsequently, in the [01-tcga-etl](./01-tcga-etl) notebook, we establish tables and publish them to Unity Catalog. Alternatively, [01-tcga-dlt](https://github.com/databricks-industry-solutions/hls-tcga/blob/main/01-tcga-dlt.py) can be employed to accomplish the same tasks using DLT pipelines.

- In [02-rna-tcga-analysis](./02-rna-tcga-analysis), we provide examples illustrating data exploration using `sql` and `pyspark-ai` to interact with the tables through natural language. 

<img src="https://hls-eng-data-public.s3.amazonaws.com/img/tcga-umap.png" width=500 >

- In the subsequent phase, within [03-rna-tcga-expression-profiles](./03-rna-tcga-expression-profiles), we curate a dataset of normalized gene expressions for each sample. We then select the most variable features, apply UMAP dimensionality reduction to these features for data visualization, and design an interactive dashboard for exploratory RNA cluster analysis.

### Workflow

[![](https://mermaid.ink/img/pako:eNptkt1O4zAQhV_FMuoGpCKVP7H4Ail1W4QECO1uuIq0GuJp18Kxg-1cVKjvzthpoSvIRTKZ8834yDNvvHEKueCj0VttGdNWR8FyyFgR_2GLhWDFMwQsxvvZJ_Aang2G4gMnCZro_PRllWoOzk-vruaTXRmpndct-PXUeYVeOuN85i7lhJ49zmiLn_Lk_OziYrYnB2ycVdTok_lZynKx2GMi-qj_Q6bl_GQhi4HYpA-9NqNRbWsb8LVH2-BMw8pDOzAdUItGd2Aju5nJv-XjbWAQUsxS_JV6cqZv8Rth_ufua7KSqVtFF75mEiIYt2KHlTz6hrwvH5PNlP9wcnx8fb09ULBfDyXrvFtqGsiPFiMoajgUbJmEkw3BFJoILObZDQSlk1pJsfMx5CuZ03Q4VemWeVR9E7XbOklCBgL6wUFj-kAXT235mLfoW9CKViuvR83z2tRcUKhwCb2JNacJENp3ZBfnStPycBF9j2MOfXS_17bZ_Q_MdkBcLMEEymKuuR9WOG_y5h2JYuDQ?type=png)](https://mermaid.live/edit#pako:eNptkt1O4zAQhV_FMuoGpCKVP7H4Ail1W4QECO1uuIq0GuJp18Kxg-1cVKjvzthpoSvIRTKZ8834yDNvvHEKueCj0VttGdNWR8FyyFgR_2GLhWDFMwQsxvvZJ_Aang2G4gMnCZro_PRllWoOzk-v)

#### Data Download
We use the following enedpoints to download open access data:

cases_endpt: https://api.gdc.cancer.gov/cases

files_endpt: https://api.gdc.cancer.gov/files

data_endpt:  https://api.gdc.cancer.gov/data

[![](https://mermaid.ink/img/pako:eNplkk1rwzAMhv-K8Ai5dNBtLVtzGDRfZYdBYWWXegc3lleDYwfbYS2l_31OQteU-WDL0vNiWdKJVIYjSUgUnagGkFr6BHoTIPZ7rDFOIN4xh_Fk7P1kVrKdQhf_4SHUWFkze8yMMrbT3c0eF4tiepFeiQ0e_JUSQvxHUmM52iv0nE3DGnFKaryGp7On-TwfhR1WRvObbF6W2bIsR4xH6-UNki6LhzKLB-LcHWE7RxHV35Y1e9ikVKfb3PxoZRiHLCQhK6aAUv2mhbE189JoENbUsMqzL6qzbYm-2sMKNUJxaCw61yFBsbZGyFDDjoL7-1fIt2u0TjoPZecP7YBQbaiUaXmA0gGimkxIjeEtyUPr-vJT0reFkiSYHAVrlackpB7QtuHMY8GlN5Yk3rY4Iaz15uOoq8t9YHLJwjdrkgimXPBir3kfRqSflPMvKLewvw?type=png)](https://mermaid.live/edit#pako:eNplkk1rwzAMhv-K8Ai5dNBtLVtzGDRfZYdBYWWXegc3lleDYwfbYS2l_31OQteU-WDL0vNiWdKJVIYjSUgUnagGkFr6BHoTIPZ7rDFOIN4xh_Fk7P1kVrKdQhf_4SHUWFkze8yMMrbT3c0eF4tiepFeiQ0e_JUSQvxHUmM52iv0nE3DGnFKaryGp7On-TwfhR1WRvObbF6W2bIsR4xH6-UNki6LhzKLB-LcHWE7RxHV35Y1e9ikVKfb3PxoZRiHLCQhK6aAUv2mhbE189JoENbUsMqzL6qzbYm-2sMKNUJxaCw61yFBsbZGyFDDjoL7-1fIt2u0TjoPZecP7YBQbaiUaXmA0gGimkxIjeEtyUPr-vJT0reFkiSYHAVrlackpB7QtuHMY8GlN5Yk3rY4Iaz15uOoq8t9YHLJwjdrkgimXPBir3kfRqSflPMvKLewvw)

#### ETL
After landing the files in a managed volume, we transform the data into the following tables:

[![](https://mermaid.ink/img/pako:eNqFk1Fr2zAQx7-K0Ah-SUe2tWz1wyB1lFHoYCxbKdjDKNY5FdiSkeTRUvLde7YjW04f6gdb-v9_p9PJuhdaaAE0povFS6YIkUq6mPRDQiL3CDVEMYn23EK0DNV7biTfV2CjEUerMbLm5jnRlTZd3IfLz9fXbOVDJ-IPPLmJKsvyLXKjjQAzQV-TFT4BV0kFk726_HJ1tQlsC4VWYrabb-tkvd0GjAPj5Ay5WbNP2yQaiGP3wddxscjUwfDmkdz9Hizb7gfhb0LOlHtdtTXYQU3SAs_OklLiWf0bNJbCU2PAWqnVYJAaHBfccU_8CpHG6DAclBgGZ3l3Bf4Y7vOeEvsFw5x5v1ouValHO0yYnyXc3KZC8oPSwXoPHa9tayaKpQJq3e9FFl79cbdLD6Agr-A_VHmQxDruPLVDyvK6qd7hxtL7walScnHxHes9baybMeaPsfewuhFG-iNKm1tf-HzuffYw9_18jGdn8cxnYF1SlLDwuYA10iWtwdRcCuy4vmsy2ndTRmMcCih5W7mM4o1DtG3wRgAT0mlDY2daWFLeOr17VgWNS15Z8NAGf4_h9ahCH_RzaO2-w4-vPW4yHw?type=png)](https://mermaid.live/edit#pako:eNqFk1Fr2zAQx7-K0Ah-SUe2tWz1wyB1lFHoYCxbKdjDKNY5FdiSkeTRUvLde7YjW04f6gdb-v9_p9PJuhdaaAE0povFS6YIkUq6mPRDQiL3CDVEMYn23EK0DNV7biTfV2CjEUerMbLm5jnRlTZd3IfLz9fXbOVDJ-IPPLmJKsvyLXKjjQAzQV-TFT4BV0kFk726_HJ1tQlsC4VWYrabb-tkvd0GjAPj5Ay5WbNP2yQaiGP3wddxscjUwfDmkdz9Hizb7gfhb0LOlHtdtTXYQU3SAs_OklLiWf0bNJbCU2PAWqnVYJAaHBfccU_8CpHG6DAclBgGZ3l3Bf4Y7vOeEvsFw5x5v1ouValHO0yYnyXc3KZC8oPSwXoPHa9tayaKpQJq3e9FFl79cbdLD6Agr-A_VHmQxDruPLVDyvK6qd7hxtL7walScnHxHes9baybMeaPsfewuhFG-iNKm1tf-HzuffYw9_18jGdn8cxnYF1SlLDwuYA10iWtwdRcCuy4vmsy2ndTRmMcCih5W7mM4o1DtG3wRgAT0mlDY2daWFLeOr17VgWNS15Z8NAGf4_h9ahCH_RzaO2-w4-vPW4yHw)

#### Analysis
To create gene expression profiles, we group profiles by sample and calculate gene-level and sample-level summary statistics and select the most variable sites for visualization of the data using UMAP. 

[![](https://mermaid.ink/img/pako:eNplk11v2jAUhv_KkSeUG1rRrtXWXEyCANvFQNM-ehNXkYlPUkv-iGynowL--04SKFSLosQ-53nfcxLbO1Y6iSxlo9GOWwBlVUyhHwIk8RkNJikkGxEwGV9GH4VXYqMxJG84pRqvjPCvmdPOd7oPd7cPD4vJSXomfuM2nqmqqv5HZs5L9GfoUzah64LTyuI5Pbn7eH8_v0gHLJ2V77r5PM2my-UFE9FH9Q6ZTRc3yywZiEP3osdhNOK29qJ5hu8_uZ3mNVosNL6gLnDbeAxBOVuEKGJ4gqvrqy-wD6ixjJzb6BpYg3EhAs1ejv8NOouwhywvXWtjgMp5Sq9h0KEcgCduZ_lFica7StFfP1VZQRCm0b0Tt9kxGr2wgQwNOa7Au7-hty6dbo0N-3m-2q4pYkT0aksl5oNusfuzmv6g7128uR-10BUtlByXNyCshPK2Mwv7ZY5mg1IqW3etfs2lErV14dxgVCG0WDhPdy1s4SoaqFpZcj0udHEUKfIDbpeD8lveNUNUqdtA69Tx2sWuDBszg94IJWnj9puPs35TcpbSUGIlWh05o4UjtG2kiLiQKjrP0uhbHDPRRvfr1Zan-cDMqQ8vDEsroQNFsdeshgPSn5PDPyQQDsY?type=png)](https://mermaid.live/edit#pako:eNplk11v2jAUhv_KkSeUG1rRrtXWXEyCANvFQNM-ehNXkYlPUkv-iGynowL--04SKFSLosQ-53nfcxLbO1Y6iSxlo9GOWwBlVUyhHwIk8RkNJikkGxEwGV9GH4VXYqMxJG84pRqvjPCvmdPOd7oPd7cPD4vJSXomfuM2nqmqqv5HZs5L9GfoUzah64LTyuI5Pbn7eH8_v0gHLJ2V77r5PM2my-UFE9FH9Q6ZTRc3yywZiEP3osdhNOK29qJ5hu8_uZ3mNVosNL6gLnDbeAxBOVuEKGJ4gqvrqy-wD6ixjJzb6BpYg3EhAs1ejv8NOouwhywvXWtjgMp5Sq9h0KEcgCduZ_lFica7StFfP1VZQRCm0b0Tt9kxGr2wgQwNOa7Au7-hty6dbo0N-3m-2q4pYkT0aksl5oNusfuzmv6g7128uR-10BUtlByXNyCshPK2Mwv7ZY5mg1IqW3etfs2lErV14dxgVCG0WDhPdy1s4SoaqFpZcj0udHEUKfIDbpeD8lveNUNUqdtA69Tx2sWuDBszg94IJWnj9puPs35TcpbSUGIlWh05o4UjtG2kiLiQKjrP0uhbHDPRRvfr1Zan-cDMqQ8vDEsroQNFsdeshgPSn5PDPyQQDsY)
