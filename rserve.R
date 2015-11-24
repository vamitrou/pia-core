#!/usr/bin/Rscript

args <- commandArgs(trailingOnly = TRUE)

say_shit <- function(shit_to_say) {
	print(shit_to_say)
}


load_data <- function(path) {
	df <- dget(path)
	return(df)
}

print_data <- function(df) {
	print(df)
}


if (!require("Rserve")) install.packages("Rserve", repos="http://cran.rstudio.com")

if (system("ps aux | grep [R]serve") == 0) {
	print("Rserve is running.")
} else {
	library(Rserve)
	run.Rserve(port=args[1])
}
