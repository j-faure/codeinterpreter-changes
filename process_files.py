import glob
import pandas as pd
import re
from datetime import datetime
from packaging import version
from typing import List, Tuple


def parse_requirements(file_path: str) -> pd.DataFrame:
    """
    Returns a DataFrame with package names and versions from a requirements file.
    """
    with open(file_path, 'r') as file:
        lines = file.readlines()

    data = [line.strip().split('==') for line in lines if '==' in line]
    df = pd.DataFrame(data, columns=['Package', 'Version'])

    return df


def find_previous_version(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds a column to a DataFrame with the previous version of the package.
    """
    df['Prev_Version'] = (
        df
        .sort_values(by=['Date', 'Package'])
        .groupby('Package')['Version']
        .shift(1)
    )
    return df


def determine_change(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds a column to a DataFrame with the change in version from the previous date.
    """
        
    def compare_versions(row):
        if pd.isna(row['Prev_Version']):
            return 'New'
        
        current_version = version.parse(row['Version'])
        previous_version = version.parse(row['Prev_Version'])
        if current_version > previous_version:
            return 'Upgraded'
        elif current_version < previous_version:
            return 'Downgraded'
        else:
            return None
    
    df['Change'] = df.apply(compare_versions, axis=1)

    return df.drop(columns=['Prev_Version'])


def generate_tables(requirements_files: List[str]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Takes a list of requirements files and returns two Markdown tables for analysis
    """
    all_data = []
    for file in requirements_files:
        df = parse_requirements(file)
        df['Date'] = datetime.strptime(re.search(r'\d{8}', file).group(), '%Y%m%d').date()
        all_data.append(df)

    full_df = pd.concat(all_data, ignore_index=True)

    # Main table
    main_table = (
        full_df
        .pivot(index='Package', columns='Date', values='Version')
        .fillna('-')
        .reset_index()
        .sort_values('Package', key=lambda x: x.str.lower())
    )

    # Changes table
    changes_table = (
        full_df
        .pipe(find_previous_version)
        .pipe(determine_change)
        .reset_index(drop=True)
        .dropna(subset=['Change'])
        .assign(Package_lower=lambda df: df['Package'].str.lower()) # Workaround for sorting
        .sort_values(by=['Date', 'Package_lower'], ascending=[False, True])
        .drop(columns=['Package_lower'])
    )
    
    return main_table, changes_table


if __name__ == "__main__":
    requirements_files = glob.glob('data/requirements_*.txt')
    main_table, changes_table = generate_tables(requirements_files)

    main_table.to_markdown('main_table.md', index=False)
    changes_table.to_markdown('changes_table.md', index=False)
