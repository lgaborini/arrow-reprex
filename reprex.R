f_dataset_merged <- tempfile()
if (fs::is_dir(f_dataset_merged)){
   unlink(f_dataset_merged, recursive = TRUE)
}

# Read ------------------------------------------------------------------------------------------------------------

tbl_input <- readr::read_rds("data_reprex_small.rds")

# It prints correctly
tbl_input
tbl_input$var_1
tbl_input$var_2

# Input number of rows
nrow(tbl_input)

# Input queries: OK

tbl_input |> dplyr::filter(var_2 == "FIL") |> dplyr::collect()

tbl_input |> dplyr::count(var_1) |> dplyr::collect()

# Write ------------------------------------------------------------------------------------------------------------

stopifnot(!fs::is_dir(f_dataset_merged))

tbl_input |>
   arrow::write_dataset(
      path = f_dataset_merged,
      partitioning = "var_2"
   )


# Read from written -----------------------------------------------------------------------------------------------

tbl_written <- arrow::open_dataset(
   sources = f_dataset_merged,
   partitioning = arrow::hive_partition()
)

# Queries that might or may not fail

# Different number of rows
nrow(tbl_written)

tbl_written |> dplyr::filter(var_2 == "FIL") |> dplyr::collect()

tbl_written |> dplyr::count(var_1) |> dplyr::collect()


