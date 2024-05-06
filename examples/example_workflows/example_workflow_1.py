# df_processed = (df
#     .pipe(dp.HClean.headers_rename, rename_cols)
#     .pipe(dp.CClean.columns_to_string, string_cols)
#     .pipe(dp.CClean.columns_to_float, float_cols)
#     .pipe(dp.CClean.columns_optimise_numerics)
#     .pipe(dp.CClean.columns_to_categorical, category_cols)
# )