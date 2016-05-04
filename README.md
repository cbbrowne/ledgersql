# ledgersql
Integration of Ledger with PostgreSQL

The notion of this is to allow drawing sets of Ledger data (http://ledger-cli.sh.org) into a PostgreSQL database.

Part of the point of "sets" is to explore consolidating data from multiple data sources, as would happen if an organization used a series of Ledger files.
For instance:
  - Software In The Public Interest might collect accounting data from various sub-projects
  - Debian might collect accounting data from sub-projects as well as from conference organizations for sponsored conferences
  
Express goals include:
  - Capturing provenance of data sets.
  - Finding overlaps, that might be expected in a couple ways:
    - Transfers from parent org to sub-org
    - If a data file is loaded twice, it would be nice to not merely duplicate the data...
