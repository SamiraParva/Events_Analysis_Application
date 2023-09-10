# Events Analysis Application: Data Insights

## Context

In the realm of data analysis, this project delves into a comprehensive examination of a data processing pipeline designed to parse input event files from local sources. The primary objective is to gain deep insights into the data through meticulous processing, applying transformations, and ultimately exporting the output for an in-depth analysis.

The datasets under scrutiny (`appEventProcessingDataset.tar.gz`) comprise three CSV files, each holding a trove of valuable information:

1. Client HTTP endpoint polling event data for a set of devices running a web application.
2. Internet connectivity status logs for the aforementioned devices, generated whenever a device goes offline while executing the application.
3. Orders data detailing dispatched orders to devices operating the aforementioned web application.

This analysis aims to answer critical questions pertaining to the connectivity environment of devices surrounding the moment an order is dispatched. It dives into the intricacies of polling event data, status codes, error codes, and connectivity status. These insights are derived across varying time windows, offering a holistic view that empowers informed decision-making and strategy development.

Through this exploration, we aim to uncover patterns, trends, and anomalies that will illuminate the business landscape, enabling data-driven actions and strategies.


## Requirements

The business team is interested in understanding the connectivity environment of a device in the period surrounding when an order is dispatched to it.

For each order dispatched to a device, the following information is required:

* The total count of all polling events.
* The count of each type of polling `status_code`.
* The count of each type of polling `error_code`, along with the count of responses without error codes.

These requirements should be fulfilled for the following periods of time:

* Three minutes before the order creation time.
* Three minutes after the order creation time.
* One hour before the order creation time.

Additionally, across an unbounded period of time, the following information is needed:

1. The time of the polling event immediately preceding and following the order creation time.
2. The most recent connectivity status ("ONLINE" or "OFFLINE") before an order, and the time at which the order changed to this status. This can be across any period of time before the order creation time. Note that not all devices have a connectivity status.

# Solution

## Description

This solution is implemented in Python and involves reading data from local sources, processing it, applying transformations, and storing the final results in a CSV-formatted output dataset.

## Getting Started

This section explains how to run this application. The process is kept simple for ease of use.

### Prerequisites

The following prerequisites are required:

* Python 3
* Internet connection to download required libraries.

### Installation

Follow these steps to run the application:

1. Install required dependencies by running the following command:

   ```sh
   $ pip install -r /path/to/requirements.txt

2. Execute the main script: 
   ```sh
   $ python Main.py
   ```

## Author

👤 **Samira Parvaniani**

- Github: [@SamiraParva](https://github.com/SamiraParva)

## Version History
* 0.1
    * Initial Release
