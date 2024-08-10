# recipes-etl
 Technical assessment for application for the Analytics Engineer (Logistics) position at HelloFresh.

# Recipes ETL

## Description
The **Recipes ETL** project performs ETL (Extract, Transform, Load) operations on a dataset of recipes, specifically filtering for those that contain variations of "Chilies" in the ingredients. This project utilizes PySpark for processing large datasets efficiently.

## Features
- Extracts recipe data from a JSON file.
- Transforms the data by:
  - Splitting ingredients into an array.
  - Filtering recipes based on the presence of "Chilies" or its variations.
  - Calculating cooking and preparation times.
  - Assigning difficulty levels based on total time.
- Loads the filtered dataset into a single CSV file for easy access.

## Requirements
- **Python 3.11.4**: Ensure you have Python installed.
- **Java**: PySpark requires Java to be installed on your machine. You can install it via:
  ```bash
  sudo apt-get install default-jdk  # for Ubuntu/Debian
  brew install openjdk  # for macOS
- **PySpark**: This project uses PySpark for data processing.

## Installation
1. Ensure you have Python 3.11.4 installed:
    ```bash
    python --version
2. Clone this repository:
    ```bash
    git clone https://github.com/v04b11/recipes-etl.git
    cd recipes-etl
3.  Install the required Python packages:
    ```bash
    pip install -r requirements.txt

##  Usage
1. Place your recipes.json file in the same directory as the script.
2. Run the script:
    ```bash
    python recipe_extractor.py
3. After execution, you will find a file named recipes_with_chilies.csv in the same directory, which contains the filtered recipes.
