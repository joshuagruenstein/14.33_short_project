# Climate Voter Turnout Model Code

Analysis lives in `analysis.ipynb`.  All data processing and generation lives in `climate_data.py`, which is commented.  Most supplemental data used or generated has been committed to `extra_data/`, with the exception of the US county boundary data which was too large to commit.  A link to this CSV file can be found in `climate_data.py`, along with the method to convert it into JSON for further processing.

In order to generate the US state figure at the end of the notebook, you will need geoplot.  To install this on Ubuntu 18.04, I had to run the following supplemental commands:

```
sudo apt-get install libgeos++-dev
sudo apt-get install libproj-dev
```