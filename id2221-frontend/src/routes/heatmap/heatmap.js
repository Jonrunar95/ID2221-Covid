import React, { Component } from 'react';
import { Link } from 'react-router-dom';
import './heatmap.css';
import week3 from './../../week3.png'


export default class Frontpage extends Component {
	constructor(props) {
		super(props);
		this.state = { 
			data: null,
			week: 40,
			loading: true, 
			error: false, 
			visible: false,
			searchValue: ''
		};
		this.handleClick = this.handleClick.bind(this);
	}
	
	async componentDidMount() {
		try {
			const {week} = this.state
			const data = await this.fetchData(week);
			this.setState({ data: data, loading: false});
		} catch(err) {
			console.error(err);
			this.setState({ error: true, loading: false });
		}
	}

	async fetchData(week) { 
		const response = await fetch('/heatmap/week');
		const data = await response.text();
		return data
	}

	async handleClick(event) {
		console.log(event.target.id)
		this.setState({loading: true, error: false })
		try {
			const data = await this.fetchData(event.target.id);
			this.setState({ data: data, loading: false});
		} catch(err) {
			console.error(err);
			this.setState({ error: true, loading: false });
		}
	}

	createButtons() {
		const array = [];
		for (let i = 3; i < 44; i += 1) {
		  array.push(
			<div className="Button" id={i} key={i}> {i} </div>
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
			<div>
				<div className="Image">
					<img src={data} alt=""></img>
				</div>
				<div className="Buttons" onClick={this.handleClick}>
					{this.createButtons()}
				</div>
			</div>

		  );
	}
}
