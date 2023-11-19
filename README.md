# Housing Price Data pipeline

## Project Structure

```
housing_analysis/
│
├── data/
│   ├── cali_housing_old.csv
│   └── cali_housing_new.csv
│
├── src/
│   ├── main.py
│   ├── data_reader.py
│   ├── data_processor.py
│   └── data_writer.py
│
├── tests/
│   ├── test_data_reader.py
│   ├── test_data_processor.py
│   └── test_data_writer.py
│
├── output/
│   ├── avg_price_by_location/
│   └── json_data/
│
├── requirements.txt
└── setup.py
```

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/ShubhamBhut/pyspark-pipeline
   cd housing_analysis
   ```

2. Install dependencies using `pip`:
   ```bash
   pip install -r requirements.txt
   ```

- The distributable package is available in `dist/` directory.

## Usage

### Running the Tests

Run tests using `unittest`:
```bash
python -m unittest discover -s tests -p 'test_*.py'
```

### Running the Pipeline

1. Navigate to the project directory:
   ```bash
   cd housing_analysis
   ```

2. Run the main pipeline:
   ```bash
   python src/main.py
   ```

### Output

- Processed data is saved in the `output/` directory.
- Check `output/avg_price_by_location/` for aggregated housing price data.
- `output/json_data/` contains JSON-formatted data.

## Testing

- The `tests/` directory contains test modules for data reading, processing, and writing.
- Run tests with:
    ```
    python -m unittest discover -s tests -p 'test_*.py'
    ```

## Contributing

- Contributions, bug reports, and suggestions are welcome. 
- Fork the repository, make changes, and submit a pull request.

## License

This project is licensed under the [MIT License](LICENSE).
