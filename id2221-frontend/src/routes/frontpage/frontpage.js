import React, { Component } from 'react';
import { Link } from 'react-router-dom';
import './frontpage.css';

export default class Frontpage extends Component {
	constructor(props) {
		super(props);
		this.state = { 
			fullData: null,
			data: null,
			loading: true, 
			error: false, 
			visible: false,
			searchValue: ''
		};
		this.handleSearch = this.handleSearch.bind(this);
	}
	
	async componentDidMount() {
		try {
			const data = await this.fetchData();
			this.setState({ fullData: data.data, data: data.data, loading: false});
		} catch(err) {
			console.error(err);
			this.setState({ error: true, loading: false });
		}
	}

	async fetchData() {
		const response = await fetch("/frontpage");
		const data = await response.json();
		return data
	}

	handleSearch(event) {
		const { fullData } = this.state;
		const str = event.target.value;
		console.log(str)
		const searchData = [];
		
		this.setState({ searchValue: str });
		if (str === '') {
		  	this.setState({ data: fullData });
		} else {
			for (let i = 0; i < fullData.length; i += 1) {
				if (fullData[i].Country.toLowerCase().includes(str.toLowerCase())) {
					searchData.push(fullData[i]);
				}
			}
			this.setState({ data: searchData });
		}
	  }

	createTable(data) {
		const array = [];
		for (let i = 0; i < data.length; i += 1) {
		  array.push(
			<tr className="tr" key={i}>
			  <td className="td td_hover"><Link to={"Country/" + data[i].Country} className="link">{data[i].Country}</Link></td>
			  <td className="td">{data[i].Date}</td>
			  <td className="td">{data[i].NewConfirmed}</td>
			  <td className="td">{data[i].TotalConfirmed}</td>
			  <td className="td">{data[i].NewDeaths}</td>
			  <td className="td">{data[i].TotalDeaths}</td>
			  <td className="td">{data[i].Population}</td>
			  <td className="td">{data[i].DeathRate}</td>
			</tr>,
		  );
		}
		return array;
	  }

	render() {
		const { data, loading, error, searchValue } = this.state;
		if (loading) {
		  return (<div>Loading data..</div>);
		}
		if (error) {
		  return (<div>Error</div>);
		}
		return (
			<div className="Frontpage">
				<div>
					<input
						type="text"
						value={searchValue}
						className={"searchbox"}
						name="searchBox"
						placeholder="Leit"
						onFocus={(e) => { e.target.placeholder = ''; }}
						onBlur={(e) => { e.target.placeholder = 'Leit'; }}
						onChange={this.handleSearch}
					/>
				</div>
				<div className="Table">
					<table className = "table">
						<thead>
						<tr className="tr">
							<th className="th">Country.</th>
							<th className="th">Date</th>
							<th className="th">NewConfirmed</th>
							<th className="th">TotalConfirmed</th>
							<th className="th">NewDeaths</th>
							<th className="th">TotalDeaths</th>
							<th className="th">Population</th>
							<th className="th">DeathRate</th>
						</tr>
						</thead>
						<tbody>
						{this.createTable(data)}
						</tbody>
					</table>
				</div>
			</div>
		  );
	}
}
