import React, { Component } from 'react';
import mockData from '../../data2.json'
import './country.css';
import plot from './IcelandPlot.png'

export default class Country extends Component {
	state = { 
		data: null, 
		loading: true, 
		error: false, 
		visible: false
	};
	
	async componentDidMount() {
		try {
			const data = await this.fetchData();
			this.setState({ data, loading: false});
		} catch(err) {
			console.error(err);
			this.setState({ error: true, loading: false });
		}
	}

	async fetchData() {
		//const response = await fetch("localhost:8888/frontpage");
		//const data = await response.json();
		return mockData; 
	}



	createTable(data) {
		const array = [];
		console.log(data)
		console.log(data.length)
		for (let i = 0; i < data.length; i += 1) {
		  array.push(
			<tr className="tr" key={i}>
			  <td className="td">{data[i].Date}</td>
			  <td className="td">{data[i].Confirmed}</td>
			  <td className="td">{data[i].Active}</td>
			  <td className="td">{data[i].Deaths}</td>
			  <td className="td">{data[i].ConfirmedLast14}</td>
			  <td className="td">{data[i].DeathsLast14}</td>
			</tr>,
		  );
		}
		return array;
	  }

	render() {
		const { data, loading, error } = this.state;
		if (loading) {
		  return (<div>Loading data..</div>);
		}
	
		if (error) {
		  return (<div>Error</div>);
		}
		return (
			<div className="Main">
				<div className="Table">
					<table className = "table">
						<thead>
						<tr className="tr">
							<th className="th">Date</th>
							<th className="th">Confirmed</th>
							<th className="th">Active</th>
							<th className="th">Deaths</th>
							<th className="th">R</th>
							<th className="th">R-Deaths</th>
						</tr>
						</thead>
						<tbody>
						{this.createTable(data.data)}
						</tbody>
					</table>
				</div>
				<div className="Images">
					<img src={plot} alt=" " />
					<img src={plot} alt=" " />
					<img src={plot} alt=" " />
				</div>
			</div>
		  );
	}
}
