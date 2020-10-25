import React from 'react';
import {
	Switch, Route, BrowserRouter
} from 'react-router-dom';
import Frontpage from './routes/frontpage';
import Country from './routes/country'
function App() {
	return (
		<div>
			<BrowserRouter>
				<Switch>
					<Route path="/Country/" component={Country} />
					<Route path="/" component={Frontpage} />
				</Switch>
			</BrowserRouter>
	  	</div>
	);
}

export default App;
