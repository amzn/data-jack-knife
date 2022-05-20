## Data Jack Knife

NOTE: This project does not yet build.  The unit test need to be rewritten before this happens.

Third-party dependencies:
Log4j - Apache License, Version 2.0
Slf4j - MIT license
JakartaCommons-lang - Apache License, Version 2.0
JakartaCommons-IO - Apache License, Version 2.0
JakartaCommons-compress - Apache License, Version 2.0
GoogleGuava - Apache License, Version 2.0

It is a generic ETL-like record processor. There are record sources, pipes and sinks. The command line tool works like unix tools, using a simple post-fix syntax, but it is multi-threaded and better suited for pretty-big data, in the 1TB or less domain. In the future, there are AWS sources and sinks for exchanging data with AWS services.

The value is two fold. Firstly, since it is a generally useful too for manipulating data. Secondly, since over time we will make available AWS sources and Sinks, it promotes using AWS and provides command line simplicity to utilizing AWS resources.

Be sure to:

* Edit your repository description on GitHub

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This project is licensed under the Apache-2.0 License.

