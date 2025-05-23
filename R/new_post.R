new_post <- function(title = "dummy",
                     root = "ramb",
                     author = "Einar Hjörleifsson",
                     filename = "index.qmd", 
                     date = Sys.Date(), 
                     categories = c("code", "rtip")) {
  # Define the base path
  base_path <- paste0(getwd(),"/", root)
  
  # Convert date to string and create the full directory path
  date_str <- as.character(date)
  full_path <- file.path(base_path, paste0(date_str, "_", title))
  
  # Create the directory if it doesn't exist
  if (!dir.exists(full_path)) {
    dir.create(full_path, recursive = TRUE)
    message("Directory created: ", full_path)
  }
  
  # Define the full file path
  file_path <- file.path(full_path, filename)
  
  # Create the content to be written in the file
  content <- paste0(
    "---\n",
    'title: "', title, '"\n',
    'author: ', author, '\n',
    'description: ""\n', 
    'date: "', date_str, '"\n',
    "categories: [", paste(categories, collapse = ", "), "]\n",
    "echo: true\n",
    # "toc: TRUE\n",
    "---\n\n"  #,
    # '<script src="https://giscus.app/client.js"\n',
    # '        data-repo="Yours_Here"\n',
    # '        data-repo-id="Yours_Here"\n',
    # '        data-category="Comments"\n',
    # '        data-category-id="Yours_Here"\n',
    # '        data-mapping="url"\n',
    # '        data-strict="0"\n',
    # '        data-reactions-enabled="1"\n',
    # '        data-emit-metadata="0"\n',
    # '        data-input-position="top"\n',
    # '        data-theme="dark"\n',
    # '        data-lang="en"\n',
    # '        data-loading="lazy"\n',
    # '        crossorigin="anonymous"\n',
    # '        async>\n',
    # '</script>\n'
  )
  
  # Write the content to the file
  writeLines(content, file_path)
  message("QMD file created: ", file_path)
}
