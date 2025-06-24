# bigo-rover-craft

This repository contains the codebase for Team Craft's rover application built as part of the internal hackathon.

## Project Overview

The core component of this project is an in-memory database, implemented from scratch in Rust. The database is designed with a strong emphasis on consistency, ensuring that all operations maintain a reliable and predictable state, even in the presence of concurrent access or failures.

### Key Features

- **In-Memory Storage:** All data is stored in memory for fast access and low latency.
- **Consistency First:** The architecture and algorithms prioritize data consistency above all else.
- **Custom Consensus Algorithm:** Implements a purpose-built consensus algorithm tailored to the application's requirements, ensuring reliable agreement across components.
- **Rust Implementation:** Built using safe and performant Rust code, leveraging the language's strengths for systems programming.
- **Custom Engine:** No external database engines or libraries are used for core functionality.

## Getting Started

1. Install [Rust](https://www.rust-lang.org/tools/install).
2. Clone this repository.
3. Build and run the project using Cargo:

   ```sh
   cargo build
   cargo run