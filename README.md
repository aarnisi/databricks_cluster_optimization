# Databricks Cluster Optimizer
Databricks Cluster Optimization: A repository focused on enhancing Databricks cluster performance, cost-effectiveness, and resource utilization.

[![License](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)

Welcome to the Databricks Cluster Optimizer! This Python class is designed to help you optimize your Databricks clusters, ensuring you get the most out of your resources while minimizing costs. With a simple and easy-to-use API, you can efficiently manage your clusters' idle time, ensuring they are up and running when needed and scaled down during periods of inactivity.

## Table of Contents

- [Introduction](#introduction)
- [Installation](#installation)
- [Usage](#usage)
- [Examples](#examples)

## Introduction

Databricks is a powerful platform for big data analytics and machine learning. However, running clusters 24/7 can be costly, especially if they remain idle during non-working hours. The Databricks Cluster Optimizer allows you to automate the process of managing cluster idle time, ensuring cost optimization without compromising performance.

## Installation

To use the Databricks Cluster Optimizer, you need to have Python installed on your system. Simply clone this repository and import the `Optimizer` class into your Python project.

python
pip install git+https://github.com/aarnisi/databricks-cluster-optimization

## Usage
The Optimizer class provides an easy-to-use interface for managing Databricks clusters. It accepts various input parameters to optimize the clusters based on your requirements. The primary input parameter is idle_time, which represents the time (in minutes) the cluster should remain idle before it is automatically terminated. Databricks personal access token is required and it should be added in os.environ["DATABRICKS_API_TOKEN"].

## Examples
Example code how to run it in Databricks notebook.
```
pip install git+https://github.com/aarnisi/databricks-cluster-optimization

import dbclusters.calculate as o
import os
os.environ["DATABRICKS_API_TOKEN"] = dbutils.secrets.get(scope="db-scope", key="personal-access-token")

o.Optimize(idle_time=10)
```
