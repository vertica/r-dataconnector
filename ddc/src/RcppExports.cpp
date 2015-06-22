// This file was generated by Rcpp::compileAttributes
// Generator token: 10BE3573-1514-4C36-9D1C-5A225CD40393

#include <Rcpp.h>

using namespace Rcpp;

// ddc_read
Rcpp::DataFrame ddc_read(const std::string& url, const Rcpp::List& options);
RcppExport SEXP ddc_ddc_read(SEXP urlSEXP, SEXP optionsSEXP) {
BEGIN_RCPP
    Rcpp::RObject __result;
    Rcpp::RNGScope __rngScope;
    Rcpp::traits::input_parameter< const std::string& >::type url(urlSEXP);
    Rcpp::traits::input_parameter< const Rcpp::List& >::type options(optionsSEXP);
    __result = Rcpp::wrap(ddc_read(url, options));
    return __result;
END_RCPP
}
