library(xtable)

dir.create("tab", showWarnings = TRUE, recursive = TRUE, mode = "0777")

options(xtable.floating=FALSE)
options(xtable.booktabs=TRUE)
options(xtable.auto=TRUE)
options(xtable.include.rownames = FALSE)
# This does not work because it thinks the option is unset.
#options(xtable.table.placement = NULL)

inlinecode <- function(x){paste0('{\\inlinecode{', x, '}}')}

add_percentage_col <- function(dt,count_col="cnt") {
  cnt_sum = sum(dt[,get(count_col)])
  dt[,percent := (get(count_col) / cnt_sum) * 100.0]
  
  return(dt)
}

nice_round <- function(val,digits) {
  smallest <- 1.0 / (10 ^ digits)
  rounded <- round(val,digits)
  format_string_lt <- sprintf("$<%%.%df$",digits)
  format_string_literal <- sprintf("$%%.%df$",digits)
  if (rounded == 0.0) {
    return(sprintf(format_string_lt, round(smallest,digits)))
  } else {
    return(sprintf(format_string_literal, rounded))
  }
}

compute_tail_addtorow <- function(dt,n,num_cols=1,count_col="cnt",with_percentage_col=TRUE) {
  t <- d[(n+1):d[,.N]]
  tail_sum<-t[,.(num=.N,cnt=sum(get(count_col)))]
  head_sum<-d[1:n][,.(num=.N,cnt=sum(get(count_col)))]
  addtorow<-list()
  addtorow$pos<-list(n)
  rounded <- nice_round((tail_sum[,cnt]/(tail_sum[,cnt] + head_sum[,cnt]))*100,2)
  
  if (with_percentage_col) {
    # We are using strings here now because the values can be int64, which does not like being formatted as %d.
    addtorow$command<-c(sprintf("\\multicolumn{%d}{c}{Others (%s)}&%s&%s\\\\\n",
                                num_cols,
                                toString(tail_sum[,num]),
                                toString(tail_sum[,cnt]),
                                rounded
    ))
  } else {
    addtorow$command<-c(sprintf("\\multicolumn{%d}{c}{Others (%s, %s\\%%)}&%d\\\\\n",
                                num_cols,
                                toString(tail_sum[,num]),
                                rounded,
                                tail_sum[,cnt]))
  }
  
  return(addtorow)
}

correctly_round_percentage_column <- function(dt,digits=2) {
  return(dt[,percent := mapply(nice_round,percent,digits)])
}