
# Table of Contents

1.  [Financial data](#org5c67bbb)
2.  [Portfolio class](#org8891bb0)
3.  [Streamlit](#orgb7bb8c5)
4.  [Tests](#orga8e13fb)
5.  [Next steps](#orgf7dc133)



<a id="org5c67bbb"></a>

# Financial data

-   Use boursorama.com website
-   Better to use an API, but as it is a simple project, navigating the source code is the way because it is free.


<a id="org8891bb0"></a>

# Portfolio class

-   Aggregate financial data in an instance of Portfolio object
-   Give stats (XIRR, quantity, dividends, quotes), charts about the assets of your portfolio/tracked assets
-   Give stats of your overall portfolio.
-   Three available periods of time for the XIRR: Year-to-Date (Ytd), last Year, since inception


<a id="orgb7bb8c5"></a>

# Streamlit

-   Use streamlit to run the webapp
-   run it by opening a terminal at the project locationby typing `streamlit run app.py`

Some screenshots :

-   Operation tab

![img](./operations_tab.png)

-   Portfolio tab

![img](./portfolio_tab1.png)
![img](./portfolio_tab2.png)


<a id="orga8e13fb"></a>

# Tests

To run tests: run in a terminal the following `python tests/unit_tests.py`


<a id="orgf7dc133"></a>

# Next steps

-   Add a way to configure data for SCPIs
-   Add manual assets, e.g. Deposit account
-   Import operations from csv, pdf

