# SQLidate

A tool to validate the equivalence of pairs of SQL queries.

## Getting Started

- Clone the repository `git clone git@github.com:yunpengn/SQLidate.git`.
- Navigate into the repository by `cd SQLidate/`.
- Build the JAR by `./gradlew shadowJar`.
- Create a copy of the configuration file by `cp config.example.properties config.properties`.
    - Remember to change the values inside `config.properties` as well.
- Supply the input in a text file, similar to the format given in `sample.txt`.
- Run the JAR by `java -jar ./build/libs/SQLidate-1.0-SNAPSHOT-all.jar <input_file_path>`.

## Licence

[GNU Public Licence 3.0](LICENSE)
