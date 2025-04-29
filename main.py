from extract import extract_csv_to_dataframe
from transform import transform_data
from load import load_data_to_referees, read_referee_data


def main():

    extracted_data = extract_csv_to_dataframe('data/E0.csv')
    transformed_data = transform_data(extracted_data)
    load_data_to_referees(transformed_data)
    print(read_referee_data())

    return


if __name__ == "__main__":
    main()
