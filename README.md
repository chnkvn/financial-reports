
# Table of Contents

1.  [Financial data](#orgfaae222)
2.  [Portfolio class](#org764195c)
3.  [Streamlit](#orgadc9785)
4.  [Tests](#org0bc1fb9)
5.  [Next steps](#orga074def)



<a id="orgfaae222"></a>

# Financial data

-   Use boursorama.com website
-   Better to use an API, but as it is a simple project, navigating the source code is the way because it is free.


<a id="org764195c"></a>

# Portfolio class

-   Aggregate financial data in an instance of Portfolio object
-   Give stats (XIRR, quantity, dividends, quotes), charts about the assets of your portfolio/tracked assets
-   Give stats of your overall portfolio.
-   Three available periods of time for the XIRR: Year-to-Date (Ytd), last Year, since inception


<a id="orgadc9785"></a>

# Streamlit

-   Use streamlit to run the webapp
-   run it by opening a terminal at the project locationby typing `streamlit run app.py`


<a id="org0bc1fb9"></a>

# Tests

To run tests: run in a terminal the following `python tests/unit_tests.py`


<a id="orga074def"></a>

# Next steps

-   Add a way to configure data for SCPIs
-   Add manual assets
-   Import operations from csv, pdf

