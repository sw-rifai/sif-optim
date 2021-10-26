using Parquet, DataFrames
DataFrame()

readdir("../../data_general/proc_sif-optim/merged_parquets")

pathof("../../data_general/proc_sif-optim/merged_parquets")


df = DataFrame("../../data_general/proc_sif-optim/merged_parquets/lc-41_region_1sif_lst_1km_2018_.parquet")