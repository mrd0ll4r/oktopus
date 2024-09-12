library(readr)
library(ggplot2)
library(xtable)
library(dplyr)
library(tidyr)
library(scales)
library(ggsci)

source("base_setup.R")
source("plot_setup.R")
source("table_setup.R")
source("tikz_setup.R")

options(xtable.caption.placement = "top")
old <- options(pillar.sigfig = 4)

#####################################
d <- read_csv("csv/num-blocks_analysis.csv.gz", col_types = "i")

save_tex_value(sprintf("$%d$", d$count), "num_blocks")

rm(d)
gc()
#####################################
d <- read_csv("csv/num-files_analysis.csv.gz", col_types = "i")

num_files_analysis <- d$count
save_tex_value(sprintf("$%d$", d$count), "num_files_analysis")

rm(d)
gc()
#####################################
d <- read_csv("csv/num-files_public.csv.gz", col_types = "i")

save_tex_value(sprintf("$%d$", d$count), "num_files_public")

rm(d)
gc()
#####################################
d <-
  read_csv("csv/num-directories_analysis.csv.gz", col_types = "fi")

check_sum_files <-
  sum((d %>% filter(unixfs_type == "file" |
                      unixfs_type == "raw"))$cnt)
stopifnot(num_files_analysis == check_sum_files)

num_directories <- (d %>% filter(unixfs_type == "directory"))$cnt
num_hamtshards <- (d %>% filter(unixfs_type == "directory"))$cnt

save_tex_value(sprintf("$%d$", num_directories), "num_directories_analysis")
save_tex_value(sprintf("$%d$", num_hamtshards), "num_hamtshards_analysis")

rm(num_hamtshards,num_directories,check_sum_files)
rm(d)
gc()
#####################################
d <-
  read_csv("csv/dag-pb-block-sizes_analysis.csv.gz", col_types = "i")

p <- ggplot(data = d, aes(x = block_size)) +
  geom_histogram(bins = 100) +
  labs(x = "Block Size (B)", y = "Number of Blocks") +
  scale_y_continuous(
    trans = "log10",
    breaks = trans_breaks("log10", function(x)
      10 ^ x),
    labels = trans_format("log10", math_format(10 ^ .x))
  ) +
  scale_x_continuous(
    trans = "log2",
    #breaks = trans_breaks("log2", function(x) 2^x, n=8),
    breaks = c(2 ^ 10, 2 ^ 18, 2 ^ 20),
    labels = trans_format("log2", math_format(2 ^ .x))
  )

print_plot(p, "dag_pb_block_size_histogram")

#d <- d[order(-cnt)]
#d <- add_percentage_col(d)
#addtorow <- compute_tail_addtorow(d,10)
#names(d) <- c("Block Size (B)", "Count","Share (%)")
#
#x <- xtable(d[1:10])
##names(x) <- c("Block Size (B)", "Count")
#print(x,file="tab/dag-pb-block-size-count.tex",add.to.row=addtorow)

rm(p,d)
gc()
#####################################
d <- read_csv("csv/raw-block-sizes_analysis.csv.gz", col_types = "i")

# Use aes(weight=cnt) for histograms with pre-aggregated counts

p <- ggplot(data = d, aes(x = block_size)) +
  geom_histogram(bins = 100) +
  labs(x = "Block Size (B)", y = "Number of Blocks") +
  scale_y_continuous(
    trans = "log10",
    breaks = trans_breaks("log10", function(x)
      10 ^ x),
    labels = trans_format("log10", math_format(10 ^ .x))
  ) +
  scale_x_continuous(
    trans = "log2",
    #breaks = trans_breaks("log2", function(x) 2^x, n=8),
    breaks = c(2 ^ 10, 2 ^ 18, 2 ^ 20),
    labels = trans_format("log2", math_format(2 ^ .x))
  )

print_plot(p, "raw_block_size_histogram")

# Combine both, create faceted plot
d2 <-
  read_csv("csv/dag-pb-block-sizes_analysis.csv.gz", col_types = "i")
d <- d %>% mutate(block_type = "raw")
d2 <- d2 %>% mutate(block_type = "dag-pb")
d <- bind_rows(d, d2)

sum_block_size_tib <- sum(d$block_size) / (1024 * 1024 * 1024 * 1024)

save_tex_value(sprintf("$%.3f$", round(mean(d$block_size), 3)), "block_size_mean")
save_tex_value(sprintf("$%d$", max(d$block_size)), "block_size_max")
save_tex_value(sprintf("$%d$", min(d$block_size)), "block_size_min")
save_tex_value(sprintf("$%.3f$TiB", round(sum_block_size_tib, 3)), "block_size_sum")

p <- ggplot(data = d, aes(x = block_size)) +
  geom_histogram(bins = 100) +
  facet_wrap(vars(block_type), dir = "v") +
  labs(x = "Block Size (B)", y = "Number of Blocks") +
  coord_trans(y = pseudo_log_trans(base = 10, sigma = 1)) +
  scale_y_continuous(
    breaks = c(100, 10000, 1000000),
    labels = trans_format("log10", math_format(10 ^ .x))
  ) +
#  scale_y_continuous(
#    trans = "log10",
#    breaks = trans_breaks("log10", function(x)
#      10 ^ x),
#    labels = trans_format("log10", math_format(10 ^ .x))
#  ) +
  scale_x_continuous(
    trans = "log2",
    #breaks = trans_breaks("log2", function(x) 2^x, n=8),
    breaks = c(2 ^ 10, 2 ^ 18, 2 ^ 20),
    labels = trans_format("log2", math_format(2 ^ .x))
  )

print_plot(p, "combined_block_size_histogram")

rm(sum_block_size_tib,d,d2,p)
gc()
#####################################
d <-
  read_csv("csv/directory-entry-counts_analysis.csv.gz", col_types = "fi")

save_tex_value(sprintf("$%.3f$", round(mean((
  d %>% filter(unixfs_type == "directory")
)$cnt), 3)),
"directory_entry_counts_directory_mean")
save_tex_value(sprintf("$%.3f$", round(mean((
  d %>% filter(unixfs_type == "HAMTShard")
)$cnt), 3)),
"directory_entry_counts_hamtshard_mean")
save_tex_value(sprintf("$%d$", max((
  d %>% filter(unixfs_type == "directory")
)$cnt)),
"directory_entry_counts_directory_max")
save_tex_value(sprintf("$%d$", max((
  d %>% filter(unixfs_type == "HAMTShard")
)$cnt)),
"directory_entry_counts_hamtshard_max")

p <- ggplot(data = d, aes(x = cnt, color = unixfs_type)) +
  stat_ecdf(pad=FALSE) +
  scale_x_continuous(
    trans = "log10",
    breaks = trans_breaks("log10", function(x)
      10 ^ x),
    labels = trans_format("log10", math_format(10 ^ .x))
  ) +
  labs(x = "Entries") +
  scale_color_jama()

print_plot(p, "directory_entry_count_ecdf")


#max_y=max(d$cnt)
#max_y = max(pretty(c(0,max_y)))
#max_y=20000
#
#p <- ggplot(data=d,aes(x=cnt,fill=unixfs_type,linetype=unixfs_type)) +
#  geom_histogram(bins=30,position="dodge")+
#  stat_ecdf(aes_(y=bquote(after_stat(y) * .(max_y)))) +
#  labs(x = "Entries", y = "Number of Directories") +
#  scale_x_continuous(trans="log10",
#                     breaks = trans_breaks("log10", function(x) 10^x),
#                     labels = trans_format("log10", math_format(10^.x))) +
#  coord_trans(y=pseudo_log_trans(base=10,sigma=1)) +
#  scale_y_continuous(breaks =c(10,100,1000,10000,20000),
#                     sec.axis = sec_axis(trans = ~ . / max_y,
#                                         #trans=pseudo_log_trans(base=10,sigma=1),
#                                         name="percentage",
#                                         breaks = c(0,0.1,0.25,0.5,1),
#                                         labels = label_percent()))
#
#print_plot(p,"directory_entry_count_histogram")

rm(p,d)
gc()
#####################################
d <-
  read_csv("csv/directory-entry-parent-counts_analysis.csv.gz",
           col_types = "i")

save_tex_value(sprintf("$%.3f$", round(mean(d$parents), 3)),
               "directory_entry_parent_count_mean")
save_tex_value(sprintf("$%d$", max(d$parents)), "directory_entry_parent_count_max")

p <- ggplot(data = d, aes(x = parents)) +
  stat_ecdf(pad = FALSE) +
  scale_x_continuous(
    trans = "log10",
    breaks = trans_breaks("log10", function(x)
      10 ^ x),
    labels = trans_format("log10", math_format(10 ^ .x))
  ) +
  labs(x = "Parents")

print_plot(p, "directory_entry_parent_count_ecdf")

#p <- ggplot(data=d,aes(x=parents)) +
#  geom_histogram(bins=20,position="dodge",boundary=0)+
#  labs(x = "Parents", y = "Number of Directory Entries") +
#  scale_x_continuous(trans="log10",
#                     breaks = trans_breaks("log10", function(x) 10^x),
#                     labels = trans_format("log10", math_format(10^.x))) +
#  coord_trans(y=pseudo_log_trans(base=10,sigma=1)) +
#  scale_y_continuous(breaks =c(10,100,1000,10000,100000,1000000,10000000),
#                     labels = trans_format("log10", math_format(10^.x)))
#
#print_plot(p,"directory_entry_parent_count_histogram")

rm(p,d)
gc()
#####################################
d <-
  read_csv("csv/file-subblock-parent-counts_analysis.csv.gz",
           col_types = "i")

save_tex_value(sprintf("$%.3f$", round(mean(d$parents), 3)), "file_subblock_parent_count_mean")
save_tex_value(sprintf("$%d$", max(d$parents)), "file_subblock_parent_count_max")

p <- ggplot(data = d, aes(x = parents)) +
  stat_ecdf(pad = FALSE) +
  scale_x_continuous(
    trans = "log10",
    breaks = trans_breaks("log10", function(x)
      10 ^ x),
    labels = trans_format("log10", math_format(10 ^ .x))
  ) +
  labs(x = "Parents")

print_plot(p, "file_subblock_parent_count_ecdf")

#p <- ggplot(data=d,aes(x=parents)) +
#  geom_histogram(bins=20,position="dodge",boundary=0)+
#  labs(x = "Parents", y = "Number of File Subblocks") +
#  scale_x_continuous(trans="log10",
#                     breaks = trans_breaks("log10", function(x) 10^x),
#                     labels = trans_format("log10", math_format(10^.x))) +
#  coord_trans(y=pseudo_log_trans(base=10,sigma=1)) +
#  scale_y_continuous(breaks =c(10,100,1000,10000,100000,1000000,10000000),
#                     labels = trans_format("log10", math_format(10^.x)))
#
#print_plot(p,"file_subblock_parent_count_histogram")

# Combine directory entries and file subblocks

d <- d %>% mutate(parent_type = "file")
d2 <-
  read_csv("csv/directory-entry-parent-counts_analysis.csv.gz",
           col_types = "i") %>% mutate(parent_type = "directory")
d <- bind_rows(d, d2)

p <- ggplot(data = d, aes(x = parents, color=parent_type)) +
  stat_ecdf(pad=FALSE) +
  scale_x_continuous(
    trans = "log10",
    breaks = trans_breaks("log10", function(x)
      10 ^ x),
    labels = trans_format("log10", math_format(10 ^ .x))
  ) +
  labs(x = "Parents") +
  scale_color_jama()

print_plot(p, "combined_parent_count_ecdf")

rm(p,d2,d)
gc()
#####################################
d <-
  read_csv("csv/block-link-counts_analysis.csv.gz", col_types = "fi") %>% filter(unixfs_type != "raw")

save_tex_value(sprintf("$%.3f$", round(mean((d %>% filter(unixfs_type == "directory"))$num_links
), 3)),
"block_link_counts_directory_mean")
save_tex_value(sprintf("$%.3f$", round(mean((d %>% filter(unixfs_type == "file"))$num_links
), 3)), "block_link_counts_file_mean")
save_tex_value(sprintf("$%.3f$", round(mean((d %>% filter(unixfs_type == "HAMTShard"))$num_links
), 3)),
"block_link_counts_hamtshard_mean")
save_tex_value(sprintf("$%d$", max((
  d %>% filter(unixfs_type == "directory")
)$num_links)), "block_link_counts_directory_max")
save_tex_value(sprintf("$%d$", max((
  d %>% filter(unixfs_type == "file")
)$num_links)), "block_link_counts_file_max")
save_tex_value(sprintf("$%d$", max((
  d %>% filter(unixfs_type == "HAMTShard")
)$num_links)), "block_link_counts_hamtshard_max")

p <- ggplot(data = d, aes(x = num_links, color = unixfs_type)) +
  stat_ecdf(pad=FALSE) +
  scale_x_continuous(
    trans = "log10",
    breaks = trans_breaks("log10", function(x)
      10 ^ x),
    labels = trans_format("log10", math_format(10 ^ .x))
  ) +
  labs(x = "Number of Links") + 
  scale_color_jama()

print_plot(p, "block_link_count_ecdf")

#max_y=max(d$num_links)
#max_y = max(pretty(c(0,max_y)))
#max_y=500000
#
#p <- ggplot(data=d,aes(x=num_links,fill=unixfs_type,linetype=unixfs_type)) +
#  geom_histogram(bins=30,position="dodge")+
#  stat_ecdf(aes_(y=bquote(after_stat(y) * .(max_y)))) +
#  labs(x = "Number of Links", y = "Count") +
#  scale_x_continuous(trans="log10",
#                     breaks = trans_breaks("log10", function(x) 10^x),
#                     labels = trans_format("log10", math_format(10^.x))) +
#  coord_trans(y=pseudo_log_trans(base=10,sigma=1)) +
#  scale_y_continuous(breaks =c(10,100,1000,10000,100000,500000),
#                     sec.axis = sec_axis(trans = ~ . / max_y,
#                                         #trans=pseudo_log_trans(base=10,sigma=1),
#                                         name="percentage",
#                                         breaks = c(0,0.1,0.25,0.5,1),
#                                         labels = label_percent()))
#
#print_plot(p,"block_link_count_histogram")

rm(p,d)
gc()
#####################################
d <-
  read_csv("csv/file-metadata-only-direntries-unique_analysis.csv.gz", col_types = "Iff")

num_unique_direntry_files_analysis <- (d %>% summarize(n=n()))$n
save_tex_value(sprintf("$%d$", num_unique_direntry_files_analysis), "num_files_only_direntries_unique_analysis")
dd <- d %>% filter(!is.na(file_size))
save_tex_value(sprintf("$%.3f$", round(mean(dd$file_size), 3)), "file_size_only_direntries_analysis_mean")
save_tex_value(sprintf("$%s$", max(dd$file_size)), "file_size_only_direntries_analysis_max")

x <- d %>% mutate(file_size = NULL) %>%
  group_by(libmagic_mime_type, freedesktop_mime_type) %>% summarise(n =
                                                                      n()) %>%
  ungroup() %>%
  mutate(Share = n / sum(n) * 100) %>%
  arrange(-n)

x_head <- x %>% head(n = 30)
x_tail <- x %>% tail(n = dim(x)[1] - 30)

tail_summary <-
  x_tail %>% summarize(num = n(),
                       cnt = sum(n),
                       Share = sum(Share))
addtorow <- list()
addtorow$pos <- list(30)
rounded <- nice_round(tail_summary$Share, 2)

# We are using strings here now because the values can be int64, which does not like being formatted as %d.
addtorow$command <-
  c(
    sprintf(
      "\\multicolumn{%d}{c}{Others (%s)}&%s&%s\\\\\n",
      2,
      toString(tail_summary$num),
      toString(tail_summary$cnt),
      rounded
    )
  )
x <- xtable(x_head)
names(x) <-
  c("libmagic MIME type",
    "freedesktop MIME type",
    "count",
    "share (%)")
print(x,
      file = "tab/file_mime_types_only_direntries_unique_analysis.tex",
      add.to.row = addtorow,
      floating = FALSE)



x <- d %>% mutate(file_size = NULL,freedesktop_mime_type=NULL) %>%
  #filter(libmagic_mime_type!= "unknown/pre-libmagic-schema") %>%
  group_by(libmagic_mime_type) %>% summarise(n = n()) %>%
  mutate(Share = n / sum(n) * 100) %>%
  arrange(-n)

x_head <- x %>% head(n = 10)
x_tail <- x %>% tail(n = dim(x)[1] - 10)

tail_summary <-
  x_tail %>% summarize(num = n(),
                       cnt = sum(n),
                       Share = sum(Share))
addtorow <- list()
addtorow$pos <- list(10)
rounded <- nice_round(tail_summary$Share, 2)

# We are using strings here now because the values can be int64, which does not like being formatted as %d.
addtorow$command <-
  c(
    sprintf(
      "\\multicolumn{%d}{c}{Others (%s)}&%s&%s\\\\\n",
      1,
      toString(tail_summary$num),
      toString(tail_summary$cnt),
      rounded
    )
  )
x <- xtable(x_head)
names(x) <-
  c("libmagic MIME type",
    "count",
    "share (%)")
print(x,
      file = "tab/file_mime_types_only_direntries_unique_analysis_libmagic.tex",
      add.to.row = addtorow,
      floating = FALSE)

rm(num_unique_direntry_files_analysis,x,d,addtorow,rounded,x_head,x_tail)
gc()
#####################################
d <-
  read_csv("csv/file-metadata-only-direntries-unique_public.csv.gz", col_types = "Iff")

num_unique_direntry_files_public <- (d %>% summarize(n=n()))$n
save_tex_value(sprintf("$%d$", num_unique_direntry_files_public), "num_files_only_direntries_unique_public")
save_tex_value(sprintf("$%.3f$", round(mean(d$file_size), 3)), "file_size_only_direntries_public_mean")
save_tex_value(sprintf("$%s$", max(d$file_size)), "file_size_only_direntries_public_max")

p <- ggplot(data = d, aes(x = file_size)) +
  stat_ecdf(pad = FALSE) +
  scale_x_continuous(
    trans = "log2",
    breaks = trans_breaks("log2", function(x)
      2 ^ x),
    labels = trans_format("log2", math_format(2 ^ .x))
  ) +
  labs(x = "File Size (B)")

print_plot(p, "file_size_only_direntries_unique_ecdf")

p <- ggplot(d, aes(x = file_size)) + geom_histogram(bins = 100) +
  labs(x = "File Size (B)", y = "Number of Files") +
  scale_x_continuous(
    trans = "log2",
    breaks = trans_breaks("log2", function(x)
      2 ^ x),
    labels = trans_format("log2", math_format(2 ^ .x))
  ) +
  coord_trans(y = pseudo_log_trans(base = 10, sigma = 1)) +
  scale_y_continuous(
    breaks = c(10, 100, 1000, 10000, 100000, 1000000, 10000000),
    labels = trans_format("log10", math_format(10 ^ .x))
  )

print_plot(p, "file_size_only_direntries_unique_histogram")

# Top k file types by libmagic, analysis database
dd <-
  read_csv("csv/file-metadata-only-direntries-unique_analysis.csv.gz", col_types = "Iff")

top_mime_types <-
  dd %>% filter(libmagic_mime_type != "unknown/pre-libmagic-schema") %>%
  mutate(freedesktop_mime_type = NULL) %>%
  group_by(libmagic_mime_type) %>%
  summarize(n = n()) %>%
  arrange(-n) %>%
  head(8)
top_mime_types <- top_mime_types$libmagic_mime_type

dd <- d %>% mutate(freedesktop_mime_type = NULL) %>% filter(libmagic_mime_type %in% top_mime_types)

p <- ggplot(dd, aes(x = file_size)) + geom_histogram(bins = 100) +
  labs(x = "File Size (B)", y = "Number of Files") +
  facet_wrap(vars(libmagic_mime_type), dir = "v", ncol = 2) +
  scale_x_continuous(
    trans = "log2",
    breaks = trans_breaks("log2", function(x)
      2 ^ x),
    labels = trans_format("log2", math_format(2 ^ .x))
  ) +
  coord_trans(y = pseudo_log_trans(base = 10, sigma = 1)) +
  scale_y_continuous(
    breaks = c(10, 1000, 100000, 10000000),
    labels = trans_format("log10", math_format(10 ^ .x))
  )

print_plot(p, "file_size_only_direntries_unique_histogram_by_file_type",height=4)

rm(num_unique_direntry_files_public,d,p,dd,top_mime_types)
gc()
#####################################
d <-
  read_csv("csv/file-metadata-only-direntries-nonunique_analysis.csv.gz", col_types = "Iff")

num_nonunique_direntry_files_analysis <- (d %>% summarize(n=n()))$n
save_tex_value(sprintf("$%d$", num_nonunique_direntry_files_analysis), "num_files_only_direntries_nonunique_analysis")

rm(num_nonunique_direntry_files_analysis,d)
gc()
#####################################
d <-
  read_csv("csv/file-metadata-only-direntries-nonunique_public.csv.gz", col_types = "Iff")

num_nonunique_direntry_files_public <- (d %>% summarize(n=n()))$n
save_tex_value(sprintf("$%d$", num_nonunique_direntry_files_public), "num_files_only_direntries_nonunique_public")

dd <-
  d %>%
  mutate(freedesktop_mime_type = NULL) %>% 
  group_by(libmagic_mime_type) %>% 
  summarize(n = n(), cum_size = as.double(sum(file_size))) %>%
  arrange(-cum_size) %>%
  mutate(mime_type=as.factor(libmagic_mime_type)) %>%
  head(20)


ggplot(dd, aes(x = "", y = cum_size, fill = mime_type)) +
  geom_col() +
  coord_polar(theta = "y")

rm(num_nonunique_direntry_files_public,d,dd)
gc()
