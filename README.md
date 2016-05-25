# SkiRaff
An ETL testing framework written in python and specialized for pygrametl. Created as a part of a bachelor project for the study group d608f16 at Aalborg University

SkiRaff is a testing framework for ETLs that provide a series of tools. It is meant for source-to-target testing of ETL programs, and can be used for automatic-, regression- and functional testing at a system level. 

Its main functionality is that it allows users to make assertions regarding a data warehouse populated by an ETL. This is done through the Predicates found in /SkiRaff/predicates/. With these predicates a user can cover the most common functional tests. Furthermore SkiRaff also provides a way for users of pygrametl to dynamically swap out hardcoded data sources and data warehouses from their ETL programs. This allows for users to provide test data sources and data warehouses for their tests more easily. This is done with the DWPopulator found in /SkiRaff/dw_populator.py


# Motivation
We found a lack in specialized software for testing ETL systems. Especially non-GUI based systems, and as such decided to create one ourselves. We decided to go for the predicate approach as we found that there was a common set of potential bugs people usually had when programming ETLs. Bugs such as duplicate rows, dropped row, referential integerity, etc. These errors often occur for ETL systems as large amounts of data is usually handled, and that developers therefor doesn't wish to check for these during the Load stage.

# pygrametl
This framework semi-depends on pygrametl, found at http://pygrametl.org/. While using pygrametl is not a necessity for using the Predicates provided by this framework, as user can themselves setup DWRepresentation objects, it is easier to how the DWPopulator perform this task on a pygrametl program.

# Contributors
As this repository is the result of a group project for d608f16 at Aalborg University, and will therefor likely not be further improved upon, we won't be interesed in contributors. If this doesn't deter you and if you are still interested, have questions or simply want to know more. Then you can contact us with the information given below.

Mathias Claus Jensen (mcje13@student.aau.dk)

Alexander Brandborg

Arash Michael Sami Kjær (ams13@student.aau.dk)

Mikael Vind Mikkelsen (mvmi12@student.aau.dk)





