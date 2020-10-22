import React, { Component } from 'react';
import mockData from '../../data.json'
import { Link } from 'react-router-dom';
import './frontpage.css';

export default class Frontpage extends Component {
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
		const { data, loading, error } = this.state;
		if (loading) {
		  return (<div>Loading data..</div>);
		}
	
		if (error) {
		  return (<div>Error</div>);
		}
		return (
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
				  {this.createTable(data.data)}
				</tbody>
			  </table>
			</div>
		  );
	}
}
