#' Format colnames for PG (i.e. lower case and replace '.' with '_')
#' @param df input dataframe
#' @return no return
#' @export
#'
#' @examples coming soon


dfColNames2lower <- function(df){
  names(df) <- gsub(x = tolower(names(df)), pattern = "\\.", replacement = "_")
  return(df)

}
