# https://www.fiskeridir.no/Tall-og-analyse/AApne-data/posisjonsrapportering-vms

# Not really working, needed to use point and mouse click
library(tidyverse)
files_historical <- 
  c("https://www.fiskeridir.no/Tall-og-analyse/AApne-data/posisjonsrapportering-vms/_/attachment/download/a6d73f29-21ad-44a1-b328-aec19fd62744:9d2f30adddd2bf8f2eb14e15e480df8df3c2415a/posisjonsrapportering-vms-2011-pos.zip",
    "https://www.fiskeridir.no/Tall-og-analyse/AApne-data/posisjonsrapportering-vms/_/attachment/download/f51512f5-5387-4311-955a-4d27503989bf:bcb1aa871de370dbbd7be50851f31d308d1e4015/posisjonsrapportering-vms-2012-pos.zip",
    "https://www.fiskeridir.no/Tall-og-analyse/AApne-data/posisjonsrapportering-vms/_/attachment/download/d963aac6-8b02-4c21-a694-ce506bd735c0:cf0e61502e0f38728e7fb81508551f9cf0023036/posisjonsrapportering-vms-2013-pos.zip",
    "https://www.fiskeridir.no/Tall-og-analyse/AApne-data/posisjonsrapportering-vms/_/attachment/download/45ce7608-5b72-4b61-9894-73a5d7ebe77c:b0fd74ccc40eefad69ba3c30a59e296ac00bca42/posisjonsrapportering-vms-2014-pos.zip",
    "https://www.fiskeridir.no/Tall-og-analyse/AApne-data/posisjonsrapportering-vms/_/attachment/download/a89b811e-76cf-4660-8d27-ee7bf44ecd1c:42441a3d6e044f750019e8c033d2041445211454/posisjonsrapportering-vms-2015-pos.zip",
    "https://www.fiskeridir.no/Tall-og-analyse/AApne-data/posisjonsrapportering-vms/_/attachment/download/fe17a8a2-eed3-43a4-868d-c84699d50953:16922efa37906d3096c174481c884d90d5e3abe1/posisjonsrapportering-vms-2016-pos.zip",
    "https://www.fiskeridir.no/Tall-og-analyse/AApne-data/posisjonsrapportering-vms/_/attachment/download/150d09d9-1d6c-4168-953d-9266b2aaa120:97ad1c34216cc01af9804fc7221f079b427f0bbc/posisjonsrapportering-vms-2017-pos.zip",
    "https://www.fiskeridir.no/Tall-og-analyse/AApne-data/posisjonsrapportering-vms/_/attachment/download/58039e8f-8b3b-469c-bafa-242f8076fefb:4fd644cf9e8c2aba415a0dc7d6e278d0e6152c39/posisjonsrapportering-vms-2018-pos.zip",
    "https://www.fiskeridir.no/Tall-og-analyse/AApne-data/posisjonsrapportering-vms/_/attachment/download/46c58ae4-ef97-4341-8091-c0e6e2a91398:349529ab7b91aec15b07f395222854257186bdf8/posisjonsrapportering-vms-2019-pos.zip",
    "https://www.fiskeridir.no/Tall-og-analyse/AApne-data/posisjonsrapportering-vms/_/attachment/download/6cba325a-4f84-4002-905a-7f25d4a6cfca:75ca7ee9be41703018c77cfc6ffa61d2d9c89b54/posisjonsrapportering-vms-2020-pos.zip",
    "https://www.fiskeridir.no/Tall-og-analyse/AApne-data/posisjonsrapportering-vms/_/attachment/download/d8736f20-309c-4b29-9786-a8d8271418c4:300bd8f940c7856c2fbeb4b2053d4fcd989f43e2/posisjonsrapportering-vms-2021-pos.zip",
    "https://www.fiskeridir.no/Tall-og-analyse/AApne-data/posisjonsrapportering-vms/_/attachment/download/7b89b19c-efc9-4db6-bfbd-134ef8c26cec:0f4a21b066526c03fe824ddf25a7370f96272f0c/VMS-2022.zip",
    "https://www.fiskeridir.no/Tall-og-analyse/AApne-data/posisjonsrapportering-vms/_/attachment/download/458958c9-b0a3-4bb6-9457-72a898f681b6:a087911b8c6b76b4cf69eab64a33f0e8250abd4a/2023-VMS.zip",
    "https://register.fiskeridir.no/vms-ers/2024-VMS.csv.zip")
base <- basename(files_historical)
for(i in 1:length(files_historical)) {
  print(base[i])
  download.file(files_historical[i], destfile = paste0("data-raw/norway/ais/", base[i]), method = "wget")
}

