# SkiRaf
An ETL testing framework written in python and specialized for pygrametl. Created as a part of a bachelor project for the study group d608f16 at Aalborg University

SkiRaf is a testing framework for ETLs that provide a series of tools. Its main functionality is that it allows users to make assertions regarding a data warehouse populated by an ETL. This is done through the Predicates found in SkiRaf/predicates/. Furthermore SkiRaf also provides a way for users of pygrametl to dynamically swap out hardcoded data sources and data warehouses from their ETL programs. This allows for users to provide test data sources and data warehouses for their tests more easily. This is done with the DWPopulator found in SkiRaf/dwpopulator/

# Example

# Installation
N/A

# pygrametl
This framework semi-depends on pygrametl, found at http://pygrametl.org/. While using pygrametl is not a necessity for using the Predicates provided by this framework, as user can themselves setup DWRepresentation objects, it is easier to how the DWPopulator perform this task on a pygrametl program.

# Contributors
As this repository is the result of a group project for d608f16 at Aalborg University, and will therefor likely not be further improved upon, we won't be interesed in contributors. If this doesn't deter you and if you are still interested, have questions or simply want to know more. Then you can contact us with the information given below.

Mathias Claus Jensen (mcje13@student.aau.dk)

Alexander Brandborg

Arash Michael Sami Kjær

Mikael Vind Mikkelsen 

# License
N/A




