library(pracma) # for fprintf

dir.create("val", showWarnings = TRUE, recursive = TRUE, mode = "0777")

now <- format(Sys.time(), tz="UTC", usetz=TRUE)
here <- as.character(Sys.info()["nodename"])

# value should be a formatted value, will be put verbose into the file at name.
save_tex_value <- function(value="\textbf{missing value}", name) {
  output_file_name <- sprintf("val/%s.tex",name)
  
  fprintf("%% Generated %s on %s\n%s%%", now, here, value, file = output_file_name)
}
