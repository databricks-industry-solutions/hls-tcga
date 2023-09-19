# Databricks notebook source
# MAGIC %md
# MAGIC # R&D Lakehouse: The Cancer Genome Atlas (TCGA)
# MAGIC <br>
# MAGIC
# MAGIC  <img src="https://www.cancer.gov/ccg/sites/g/files/xnrzdm256/files/styles/cgov_featured/public/cgov_image/media_image/2022-06/TCGA%20people%20and%20layers%20of%20data%20425x319.jpg?h=982f41e1&itok=zkQ_l8-t" width=50% >
# MAGIC  
# MAGIC [The Cancer Genome Atlas (TCGA)](https://www.cancer.gov/ccg/research/genome-sequencing/tcga) is a comprehensive and coordinated effort to accelerate our understanding of the molecular basis of cancer through the application of genome analysis technologies, including large-scale genome sequencing. TCGA was launched in 2006 by the [National Cancer Institute (NCI)](https://www.nih.gov/about-nih/what-we-do/nih-almanac/national-cancer-institute-nci) and [National Human Genome Research Institute (NHGRI)](https://www.genome.gov/).
# MAGIC
# MAGIC The goals of TCGA are:
# MAGIC 1. Improve our ability to diagnose, treat and prevent cancer. By better understanding the genomic changes involved in cancer, we can develop more effective diagnostic and treatment approaches.
# MAGIC 2. Identify molecular targets for therapy. By finding shared molecular characteristics of tumors, we can develop targeted therapies that attack those specific targets.
# MAGIC 3. Reveal mechanisms of carcinogenesis. Identifying the genomic changes involved in the development of cancer sheds light on mechanisms by which normal cells become tumors. 
# MAGIC 4. Improve cancer recurrence prediction. Learning about the genomic changes in tumors can help identify markers that indicate a higher risk of cancer coming back after treatment. 
# MAGIC 5. Enable new discoveries through data sharing. TCGA shares all its data and analyses with the entire scientific community so anyone can conduct their own research, make new discoveries and develop better solutions.
# MAGIC
# MAGIC TCGA currently has over [2.5 petabytes of genomic, epigenomic, transcriptomic, and proteomic data from 33 types of cancer](https://www.cancergenomicscloud.org/access-tcga-dataset#:~:text=The%20Cancer%20Genome%20Atlas%20(TCGA,TCGA%20is%20challenging%20to%20use.). Over 10,000 patients have contributed tumor samples and matched controls from blood or adjacent normal tissues. All data is available through the Genomic Data Commons, visually exploreable via the Integrated Genomics Viewer.
# MAGIC
# MAGIC TCGA is an unprecedented comprehensive catalog of the key genomic changes in major cancers. It continues to drive major advances in understanding cancer biology, revealing insights into mechanisms of tumorigenesis, and paving the way for new diagnostic and therapeutic approaches.
# MAGIC
# MAGIC
# MAGIC ## Workflow overview
# MAGIC In this solution accelerator, we provide a template for show how easily you can load RNA epxression profiles from [TCGA](https://portal.gdc.cancer.gov/) and clincial data associated with the samples into databricks lakehouse platform and apply different analysis on the dataset. 
# MAGIC
# MAGIC
# MAGIC <img src="https://hls-eng-data-public.s3.amazonaws.com/img/tcga-umap.png" width=500 >
# MAGIC
# MAGIC
# MAGIC - First we land RNA expession profiles along with clinical metadatae using <a href="$./00-data-download">00-data-download</a>
# MAGIC notebook which uses [GDC APIs](https://gdc.cancer.gov/access-data/gdc-data-transfer-tool) to collect associated metadata and land data in the cloude strorage.
# MAGIC
# MAGIC - Next we in the <a href="$./01-tcga-etl">01-tcga-etl</a> notebook we create tables and publish these tables to Unity Catalog. 
# MAGIC Alternatively you can use <a href="$./01-tcga-dlt">01-tcga-dlt</a> to perform the same tasks using DLT pipelines. 
# MAGIC
# MAGIC - In <a href="$./02-rna-tcga-analysis"> 02-rna-tcga-analysis </a> we show examples of exploring the data using `sql` and `pyspark-ai` for interacting with the tables in natural language. 
# MAGIC
# MAGIC - In the next step, in <a href="$03-rna-tcga-expression-profiles"> 03-rna-tcga-expression-profiles</a> we create a dataset of normalized gene expressions for each sample and select the most variable featueres and apply UMAP dimensionality reduction on these features for data visualization and create an interactive dashboard for exploratroty analysis of the RNA clsuters.
# MAGIC
# MAGIC [![](https://mermaid.ink/img/pako:eNptkt1O4zAQhV_FMuoGpCKVP7H4Ail1W4QECO1uuIq0GuJp18Kxg-1cVKjvzthpoSvIRTKZ8834yDNvvHEKueCj0VttGdNWR8FyyFgR_2GLhWDFMwQsxvvZJ_Aang2G4gMnCZro_PRllWoOzk-vruaTXRmpndct-PXUeYVeOuN85i7lhJ49zmiLn_Lk_OziYrYnB2ycVdTok_lZynKx2GMi-qj_Q6bl_GQhi4HYpA-9NqNRbWsb8LVH2-BMw8pDOzAdUItGd2Aju5nJv-XjbWAQUsxS_JV6cqZv8Rth_ufua7KSqVtFF75mEiIYt2KHlTz6hrwvH5PNlP9wcnx8fb09ULBfDyXrvFtqGsiPFiMoajgUbJmEkw3BFJoILObZDQSlk1pJsfMx5CuZ03Q4VemWeVR9E7XbOklCBgL6wUFj-kAXT235mLfoW9CKViuvR83z2tRcUKhwCb2JNacJENp3ZBfnStPycBF9j2MOfXS_17bZ_Q_MdkBcLMEEymKuuR9WOG_y5h2JYuDQ?type=png)](https://mermaid.live/edit#pako:eNptkt1O4zAQhV_FMuoGpCKVP7H4Ail1W4QECO1uuIq0GuJp18Kxg-1cVKjvzthpoSvIRTKZ8834yDNvvHEKueCj0VttGdNWR8FyyFgR_2GLhWDFMwQsxvvZJ_Aang2G4gMnCZro_PRllWoOzk-vruaTXRmpndct-PXUeYVeOuN85i7lhJ49zmiLn_Lk_OziYrYnB2ycVdTok_lZynKx2GMi-qj_Q6bl_GQhi4HYpA-9NqNRbWsb8LVH2-BMw8pDOzAdUItGd2Aju5nJv-XjbWAQUsxS_JV6cqZv8Rth_ufua7KSqVtFF75mEiIYt2KHlTz6hrwvH5PNlP9wcnx8fb09ULBfDyXrvFtqGsiPFiMoajgUbJmEkw3BFJoILObZDQSlk1pJsfMx5CuZ03Q4VemWeVR9E7XbOklCBgL6wUFj-kAXT235mLfoW9CKViuvR83z2tRcUKhwCb2JNacJENp3ZBfnStPycBF9j2MOfXS_17bZ_Q_MdkBcLMEEymKuuR9WOG_y5h2JYuDQ)

# COMMAND ----------

# DBTITLE 1,Summary of the Steps 
# MAGIC %md
# MAGIC ## Data Download
# MAGIC
# MAGIC [![](https://mermaid.ink/img/pako:eNplkk1rwzAMhv-K8Ai5dNBtLVtzGDRfZYdBYWWXegc3lleDYwfbYS2l_31OQteU-WDL0vNiWdKJVIYjSUgUnagGkFr6BHoTIPZ7rDFOIN4xh_Fk7P1kVrKdQhf_4SHUWFkze8yMMrbT3c0eF4tiepFeiQ0e_JUSQvxHUmM52iv0nE3DGnFKaryGp7On-TwfhR1WRvObbF6W2bIsR4xH6-UNki6LhzKLB-LcHWE7RxHV35Y1e9ikVKfb3PxoZRiHLCQhK6aAUv2mhbE189JoENbUsMqzL6qzbYm-2sMKNUJxaCw61yFBsbZGyFDDjoL7-1fIt2u0TjoPZecP7YBQbaiUaXmA0gGimkxIjeEtyUPr-vJT0reFkiSYHAVrlackpB7QtuHMY8GlN5Yk3rY4Iaz15uOoq8t9YHLJwjdrkgimXPBir3kfRqSflPMvKLewvw?type=png)](https://mermaid.live/edit#pako:eNplkk1rwzAMhv-K8Ai5dNBtLVtzGDRfZYdBYWWXegc3lleDYwfbYS2l_31OQteU-WDL0vNiWdKJVIYjSUgUnagGkFr6BHoTIPZ7rDFOIN4xh_Fk7P1kVrKdQhf_4SHUWFkze8yMMrbT3c0eF4tiepFeiQ0e_JUSQvxHUmM52iv0nE3DGnFKaryGp7On-TwfhR1WRvObbF6W2bIsR4xH6-UNki6LhzKLB-LcHWE7RxHV35Y1e9ikVKfb3PxoZRiHLCQhK6aAUv2mhbE189JoENbUsMqzL6qzbYm-2sMKNUJxaCw61yFBsbZGyFDDjoL7-1fIt2u0TjoPZecP7YBQbaiUaXmA0gGimkxIjeEtyUPr-vJT0reFkiSYHAVrlackpB7QtuHMY8GlN5Yk3rY4Iaz15uOoq8t9YHLJwjdrkgimXPBir3kfRqSflPMvKLewvw)
# MAGIC
# MAGIC ## ETL
# MAGIC [![](https://mermaid.ink/img/pako:eNqFk1Fr2zAQx7-K0Ah-SUe2tWz1wyB1lFHoYCxbKdjDKNY5FdiSkeTRUvLde7YjW04f6gdb-v9_p9PJuhdaaAE0povFS6YIkUq6mPRDQiL3CDVEMYn23EK0DNV7biTfV2CjEUerMbLm5jnRlTZd3IfLz9fXbOVDJ-IPPLmJKsvyLXKjjQAzQV-TFT4BV0kFk726_HJ1tQlsC4VWYrabb-tkvd0GjAPj5Ay5WbNP2yQaiGP3wddxscjUwfDmkdz9Hizb7gfhb0LOlHtdtTXYQU3SAs_OklLiWf0bNJbCU2PAWqnVYJAaHBfccU_8CpHG6DAclBgGZ3l3Bf4Y7vOeEvsFw5x5v1ouValHO0yYnyXc3KZC8oPSwXoPHa9tayaKpQJq3e9FFl79cbdLD6Agr-A_VHmQxDruPLVDyvK6qd7hxtL7walScnHxHes9baybMeaPsfewuhFG-iNKm1tf-HzuffYw9_18jGdn8cxnYF1SlLDwuYA10iWtwdRcCuy4vmsy2ndTRmMcCih5W7mM4o1DtG3wRgAT0mlDY2daWFLeOr17VgWNS15Z8NAGf4_h9ahCH_RzaO2-w4-vPW4yHw?type=png)](https://mermaid.live/edit#pako:eNqFk1Fr2zAQx7-K0Ah-SUe2tWz1wyB1lFHoYCxbKdjDKNY5FdiSkeTRUvLde7YjW04f6gdb-v9_p9PJuhdaaAE0povFS6YIkUq6mPRDQiL3CDVEMYn23EK0DNV7biTfV2CjEUerMbLm5jnRlTZd3IfLz9fXbOVDJ-IPPLmJKsvyLXKjjQAzQV-TFT4BV0kFk726_HJ1tQlsC4VWYrabb-tkvd0GjAPj5Ay5WbNP2yQaiGP3wddxscjUwfDmkdz9Hizb7gfhb0LOlHtdtTXYQU3SAs_OklLiWf0bNJbCU2PAWqnVYJAaHBfccU_8CpHG6DAclBgGZ3l3Bf4Y7vOeEvsFw5x5v1ouValHO0yYnyXc3KZC8oPSwXoPHa9tayaKpQJq3e9FFl79cbdLD6Agr-A_VHmQxDruPLVDyvK6qd7hxtL7walScnHxHes9baybMeaPsfewuhFG-iNKm1tf-HzuffYw9_18jGdn8cxnYF1SlLDwuYA10iWtwdRcCuy4vmsy2ndTRmMcCih5W7mM4o1DtG3wRgAT0mlDY2daWFLeOr17VgWNS15Z8NAGf4_h9ahCH_RzaO2-w4-vPW4yHw)
# MAGIC
# MAGIC ## Analysis
# MAGIC
# MAGIC [![](https://mermaid.ink/img/pako:eNplk11v2jAUhv_KkSeUG1rRrtXWXEyCANvFQNM-ehNXkYlPUkv-iGynowL--04SKFSLosQ-53nfcxLbO1Y6iSxlo9GOWwBlVUyhHwIk8RkNJikkGxEwGV9GH4VXYqMxJG84pRqvjPCvmdPOd7oPd7cPD4vJSXomfuM2nqmqqv5HZs5L9GfoUzah64LTyuI5Pbn7eH8_v0gHLJ2V77r5PM2my-UFE9FH9Q6ZTRc3yywZiEP3osdhNOK29qJ5hu8_uZ3mNVosNL6gLnDbeAxBOVuEKGJ4gqvrqy-wD6ixjJzb6BpYg3EhAs1ejv8NOouwhywvXWtjgMp5Sq9h0KEcgCduZ_lFica7StFfP1VZQRCm0b0Tt9kxGr2wgQwNOa7Au7-hty6dbo0N-3m-2q4pYkT0aksl5oNusfuzmv6g7128uR-10BUtlByXNyCshPK2Mwv7ZY5mg1IqW3etfs2lErV14dxgVCG0WDhPdy1s4SoaqFpZcj0udHEUKfIDbpeD8lveNUNUqdtA69Tx2sWuDBszg94IJWnj9puPs35TcpbSUGIlWh05o4UjtG2kiLiQKjrP0uhbHDPRRvfr1Zan-cDMqQ8vDEsroQNFsdeshgPSn5PDPyQQDsY?type=png)](https://mermaid.live/edit#pako:eNplk11v2jAUhv_KkSeUG1rRrtXWXEyCANvFQNM-ehNXkYlPUkv-iGynowL--04SKFSLosQ-53nfcxLbO1Y6iSxlo9GOWwBlVUyhHwIk8RkNJikkGxEwGV9GH4VXYqMxJG84pRqvjPCvmdPOd7oPd7cPD4vJSXomfuM2nqmqqv5HZs5L9GfoUzah64LTyuI5Pbn7eH8_v0gHLJ2V77r5PM2my-UFE9FH9Q6ZTRc3yywZiEP3osdhNOK29qJ5hu8_uZ3mNVosNL6gLnDbeAxBOVuEKGJ4gqvrqy-wD6ixjJzb6BpYg3EhAs1ejv8NOouwhywvXWtjgMp5Sq9h0KEcgCduZ_lFica7StFfP1VZQRCm0b0Tt9kxGr2wgQwNOa7Au7-hty6dbo0N-3m-2q4pYkT0aksl5oNusfuzmv6g7128uR-10BUtlByXNyCshPK2Mwv7ZY5mg1IqW3etfs2lErV14dxgVCG0WDhPdy1s4SoaqFpZcj0udHEUKfIDbpeD8lveNUNUqdtA69Tx2sWuDBszg94IJWnj9puPs35TcpbSUGIlWh05o4UjtG2kiLiQKjrP0uhbHDPRRvfr1Zan-cDMqQ8vDEsroQNFsdeshgPSn5PDPyQQDsY)

# COMMAND ----------

# MAGIC %md
# MAGIC ## TCGA Data Model
# MAGIC
# MAGIC [![](https://mermaid.ink/img/pako:eNp90l9rwjAQAPCvEjKkLwpuDDb7FtvYdVgtVfdUkGhPDbSJpBEm2u--JvX_xvqQJne_uxR6B7yUGWAXp6LVOqQCIS64dpHdIuToDRTguMhZsBKc9m30iynOFjmUzoXXqa3iBVN7T-ZSmbqn15dej3bPpVcxhW99VV37_FZ9qTJQV_fmPbgSllJkdze-E48MBjdGg9L8jvQJfR54TiMq86qXqtVKRSpA-ZytFSuabJyMg4RE6HjsdI4Hc_yk3hS5KMUbVqb4omzYKHlAHplQS-qP04yLi7OJE_JpZFrHH6H32O6WTUgUD-mjOEVPJqHEnwfJeBY_upvMud8w9Ok8jEjQNF1ALsW6nGv5T82sH4XTKfXnsxEZhsGo3hk1-aMDbuMCVMF4Vo-VHYwU24FJscEZrNgu10ZWNd1tM6aBZlxLhV2tdtDGbKflZC-W53NjTn8FuyuWl3UUbE3UjK-d4uoHlwnXHQ?type=png)](https://mermaid.live/edit#pako:eNp90l9rwjAQAPCvEjKkLwpuDDb7FtvYdVgtVfdUkGhPDbSJpBEm2u--JvX_xvqQJne_uxR6B7yUGWAXp6LVOqQCIS64dpHdIuToDRTguMhZsBKc9m30iynOFjmUzoXXqa3iBVN7T-ZSmbqn15dej3bPpVcxhW99VV37_FZ9qTJQV_fmPbgSllJkdze-E48MBjdGg9L8jvQJfR54TiMq86qXqtVKRSpA-ZytFSuabJyMg4RE6HjsdI4Hc_yk3hS5KMUbVqb4omzYKHlAHplQS-qP04yLi7OJE_JpZFrHH6H32O6WTUgUD-mjOEVPJqHEnwfJeBY_upvMud8w9Ok8jEjQNF1ALsW6nGv5T82sH4XTKfXnsxEZhsGo3hk1-aMDbuMCVMF4Vo-VHYwU24FJscEZrNgu10ZWNd1tM6aBZlxLhV2tdtDGbKflZC-W53NjTn8FuyuWl3UUbE3UjK-d4uoHlwnXHQ)
