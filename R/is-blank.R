#' Find values that are blank
#'
#' @param x A sql string
#' @param false.triggers Defaults to FALSE
#'
#' @return Boolean TRUE or FALSE
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
