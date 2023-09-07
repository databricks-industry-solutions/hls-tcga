# The Cancer Genome Atlas (TCGA)

 <img src="https://www.cancer.gov/ccg/sites/g/files/xnrzdm256/files/styles/cgov_featured/public/cgov_image/media_image/2022-06/TCGA%20people%20and%20layers%20of%20data%20425x319.jpg?h=982f41e1&itok=zkQ_l8-t" width=200 >
 
[The Cancer Genome Atlas (TCGA)](https://www.cancer.gov/ccg/research/genome-sequencing/tcga) is a comprehensive and coordinated effort to accelerate our understanding of the molecular basis of cancer through the application of genome analysis technologies, including large-scale genome sequencing. TCGA was launched in 2006 by the [National Cancer Institute (NCI)](https://www.nih.gov/about-nih/what-we-do/nih-almanac/national-cancer-institute-nci)] and [National Human Genome Research Institute (NHGRI)](https://www.genome.gov/).

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

[![](https://mermaid.ink/img/pako:eNptkk1PwzAMhv9KFDR6GdL4EpDDpK7bEBIgxNepEjKNNyLSpCTpYUL77zjpBqtGD23q97HzJvY3r6xELvhg8F0axpRRQbC0ZCwLH1hjJlj2Dh6z4W70FZyCd40--8VJgipYN_lcxpyDs5Orq9lom0Zq41QNbjWxTqIrrLYucRfFiJ4dTiuDf_Lo7PT8fLoje6yskVToj7nMi3w-32ECuqB6yCSfHc-LrCPW8UOv9WBQmtJ4_GrRVDhVsHRQd0wDVKJSDZjArqfFW_5w4xn4uGZxvU8V2raSPdEVwBL35ent837whS589VZAAG2XsXwKsE3gH_4uf4iWY_zX1dHReNzbXLDH-5w1zi4UteiwxgCSKnZpPTKmkjHBJOoALKSedhyFo9pzKPrOelqCyR7VUjVzKNsqKLvxGoUEeHSdu0q3ntpEm_Ehr9HVoCQNYhqmkqchK7mgpcQFtDqUnPpFaNvQUXAmFR2Ai-BaHHJog31amWr73zGbdnKxAO0piinnrhv4NPfrH0-P85g?type=png)](https://mermaid.live/edit#pako:eNptkk1PwzAMhv9KFDR6GdL4EpDDpK7bEBIgxNepEjKNNyLSpCTpYUL77zjpBqtGD23q97HzJvY3r6xELvhg8F0axpRRQbC0ZCwLH1hjJlj2Dh6z4W70FZyCd40--8VJgipYN_lcxpyDs5Orq9lom0Zq41QNbjWxTqIrrLYucRfFiJ4dTiuDf_Lo7PT8fLoje6yskVToj7nMi3w-32ECuqB6yCSfHc-LrCPW8UOv9WBQmtJ4_GrRVDhVsHRQd0wDVKJSDZjArqfFW_5w4xn4uGZxvU8V2raSPdEVwBL35ent837whS589VZAAG2XsXwKsE3gH_4uf4iWY_zX1dHReNzbXLDH-5w1zi4UteiwxgCSKnZpPTKmkjHBJOoALKSedhyFo9pzKPrOelqCyR7VUjVzKNsqKLvxGoUEeHSdu0q3ntpEm_Ehr9HVoCQNYhqmkqchK7mgpcQFtDqUnPpFaNvQUXAmFR2Ai-BaHHJog31amWr73zGbdnKxAO0piinnrhv4NPfrH0-P85g)

## Summary of the Steps 
### Data Download

[![](https://mermaid.ink/img/pako:eNplkk1rwzAMhv-K8Ai5dNBtLVtzGDRfZYdBYWWXegc3lleDYwfbYS2l_31OQteU-WDL0vNiWdKJVIYjSUgUnagGkFr6BHoTIPZ7rDFOIN4xh_Fk7P1kVrKdQhf_4SHUWFkze8yMMrbT3c0eF4tiepFeiQ0e_JUSQvxHUmM52iv0nE3DGnFKaryGp7On-TwfhR1WRvObbF6W2bIsR4xH6-UNki6LhzKLB-LcHWE7RxHV35Y1e9ikVKfb3PxoZRiHLCQhK6aAUv2mhbE189JoENbUsMqzL6qzbYm-2sMKNUJxaCw61yFBsbZGyFDDjoL7-1fIt2u0TjoPZecP7YBQbaiUaXmA0gGimkxIjeEtyUPr-vJT0reFkiSYHAVrlackpB7QtuHMY8GlN5Yk3rY4Iaz15uOoq8t9YHLJwjdrkgimXPBir3kfRqSflPMvKLewvw?type=png)](https://mermaid.live/edit#pako:eNplkk1rwzAMhv-K8Ai5dNBtLVtzGDRfZYdBYWWXegc3lleDYwfbYS2l_31OQteU-WDL0vNiWdKJVIYjSUgUnagGkFr6BHoTIPZ7rDFOIN4xh_Fk7P1kVrKdQhf_4SHUWFkze8yMMrbT3c0eF4tiepFeiQ0e_JUSQvxHUmM52iv0nE3DGnFKaryGp7On-TwfhR1WRvObbF6W2bIsR4xH6-UNki6LhzKLB-LcHWE7RxHV35Y1e9ikVKfb3PxoZRiHLCQhK6aAUv2mhbE189JoENbUsMqzL6qzbYm-2sMKNUJxaCw61yFBsbZGyFDDjoL7-1fIt2u0TjoPZecP7YBQbaiUaXmA0gGimkxIjeEtyUPr-vJT0reFkiSYHAVrlackpB7QtuHMY8GlN5Yk3rY4Iaz15uOoq8t9YHLJwjdrkgimXPBir3kfRqSflPMvKLewvw)

### ETL
[![](https://mermaid.ink/img/pako:eNqFk1Fr2zAQx7-K0Ah-SUa2tmz1wyB1lFJoYSzbKNjDKNY5FdiSkeTSUvLdd7ajWE4f6gdbuv_v9NfJujdaaAE0prPZW6YIkUq6mPRDQiL3BDVEMYl23EI0D6N_uZF8V4GNTjhKjZE1N6-JrrTp8j5dfr2-ZkufOhK_4cWNVFmW75EbbQSYEfqWLPEJuEoqGOXl5cXV1TqQLRRaicluvq-S1WYTMA6MkxPkZsW-bJJoIA7dB1-H2SxTe8ObJ3L_a5BsuxsCRaVbQazThu9h0JK0wPOypJR4Pv-GGEvhpTFgrdRqEEgNjgvuuCd-hkhjdJgOSpz5_klIpo5-R0O_UOiV96vkUpX6JIdG-ZnR-i4Vku-VDtZ77HhtWzNSLBVQ634nsvDR2_ttugcFeQXPUOWBiXXceWqLlOV1U33A9SUfCySLxQ8s87ifbsaYP7Vew6LG0yCLzxha3_l6p3Ovs8ep7uenfHaWz7wD60wxhPVOA1gandMaTM2lwKbqGyOjfcNkNMahgJK3lcsoXipE2wYvADAh8frQ2JkW5pS3Tm9fVUHjklcWPLTGv2J4fYpCn_QwdG_fxIf_Lgop1Q?type=png)](https://mermaid.live/edit#pako:eNqFk1Fr2zAQx7-K0Ah-SUa2tmz1wyB1lFJoYSzbKNjDKNY5FdiSkeTSUvLdd7ajWE4f6gdbuv_v9NfJujdaaAE0prPZW6YIkUq6mPRDQiL3BDVEMYl23EI0D6N_uZF8V4GNTjhKjZE1N6-JrrTp8j5dfr2-ZkufOhK_4cWNVFmW75EbbQSYEfqWLPEJuEoqGOXl5cXV1TqQLRRaicluvq-S1WYTMA6MkxPkZsW-bJJoIA7dB1-H2SxTe8ObJ3L_a5BsuxsCRaVbQazThu9h0JK0wPOypJR4Pv-GGEvhpTFgrdRqEEgNjgvuuCd-hkhjdJgOSpz5_klIpo5-R0O_UOiV96vkUpX6JIdG-ZnR-i4Vku-VDtZ77HhtWzNSLBVQ634nsvDR2_ttugcFeQXPUOWBiXXceWqLlOV1U33A9SUfCySLxQ8s87ifbsaYP7Vew6LG0yCLzxha3_l6p3Ovs8ep7uenfHaWz7wD60wxhPVOA1gandMaTM2lwKbqGyOjfcNkNMahgJK3lcsoXipE2wYvADAh8frQ2JkW5pS3Tm9fVUHjklcWPLTGv2J4fYpCn_QwdG_fxIf_Lgop1Q)

### Analysis

[![](https://mermaid.ink/img/pako:eNplk11v2jAUhv_KkSeUG1rRrtXWXEyCANvFQNM-ehNXkYlPUkv-iGynowL--04SKFSLosQ-53nfcxLbO1Y6iSxlo9GOWwBlVUyhHwIk8RkNJikkGxEwGV9GH4VXYqMxJG84pRqvjPCvmdPOd7oPd7cPD4vJSXomfuM2nqmqqv5HZs5L9GfoUzah64LTyuI5Pbn7eH8_v0gHLJ2V77r5PM2my-UFE9FH9Q6ZTRc3yywZiEP3osdhNOK29qJ5hu8_uZ3mNVosNL6gLnDbeAxBOVuEKGJ4gqvrqy-wD6ixjJzb6BpYg3EhAs1ejv8NOouwhywvXWtjgMp5Sq9h0KEcgCduZ_lFica7StFfP1VZQRCm0b0Tt9kxGr2wgQwNOa7Au7-hty6dbo0N-3m-2q4pYkT0aksl5oNusfuzmv6g7128uR-10BUtlByXNyCshPK2Mwv7ZY5mg1IqW3etfs2lErV14dxgVCG0WDhPdy1s4SoaqFpZcj0udHEUKfIDbpeD8lveNUNUqdtA69Tx2sWuDBszg94IJWnj9puPs35TcpbSUGIlWh05o4UjtG2kiLiQKjrP0uhbHDPRRvfr1Zan-cDMqQ8vDEsroQNFsdeshgPSn5PDPyQQDsY?type=png)](https://mermaid.live/edit#pako:eNplk11v2jAUhv_KkSeUG1rRrtXWXEyCANvFQNM-ehNXkYlPUkv-iGynowL--04SKFSLosQ-53nfcxLbO1Y6iSxlo9GOWwBlVUyhHwIk8RkNJikkGxEwGV9GH4VXYqMxJG84pRqvjPCvmdPOd7oPd7cPD4vJSXomfuM2nqmqqv5HZs5L9GfoUzah64LTyuI5Pbn7eH8_v0gHLJ2V77r5PM2my-UFE9FH9Q6ZTRc3yywZiEP3osdhNOK29qJ5hu8_uZ3mNVosNL6gLnDbeAxBOVuEKGJ4gqvrqy-wD6ixjJzb6BpYg3EhAs1ejv8NOouwhywvXWtjgMp5Sq9h0KEcgCduZ_lFica7StFfP1VZQRCm0b0Tt9kxGr2wgQwNOa7Au7-hty6dbo0N-3m-2q4pYkT0aksl5oNusfuzmv6g7128uR-10BUtlByXNyCshPK2Mwv7ZY5mg1IqW3etfs2lErV14dxgVCG0WDhPdy1s4SoaqFpZcj0udHEUKfIDbpeD8lveNUNUqdtA69Tx2sWuDBszg94IJWnj9puPs35TcpbSUGIlWh05o4UjtG2kiLiQKjrP0uhbHDPRRvfr1Zan-cDMqQ8vDEsroQNFsdeshgPSn5PDPyQQDsY)