import argparse

from simpletl import Pipeline



def main():
    parser = argparse.ArgumentParser(description="SimpleTL Pipeline Runner")
    
    parser.add_argument("config_file", help="Path to the YAML configuration file")
    args = parser.parse_args()
    
    pipeline = Pipeline.from_config_file(args.config_file)
    pipeline.run()
    
    
if __name__ == "__main__":
    main()