#' Find values that are blank
#'
#' @param x A sql string
#' @param connList Named list with the following parameters Driver,host,user,dbname,password,port,schema
#'
#' @return Boolean True or False
#' @export
#'
#' @examples is_blank('')

is_blank <- function(x, false.triggers=FALSE){
  if(is.function(x)) return(FALSE)
  return(
    is.null(x) ||                # Actually this line is unnecessary since
      length(x) == 0 ||            # length(NULL) = 0, but I like to be clear
      all(is.na(x)) ||
      all(x=="") ||
      (false.triggers && all(!x))
  )
}
