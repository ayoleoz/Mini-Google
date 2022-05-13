import React, { useState } from 'react';
import { createTheme, ThemeProvider } from '@material-ui/core';
import Search from './Search'
import Results from './Results'
import { RESULTS } from './mockData';

// Theme from https://css-tricks.com/a-dark-mode-toggle-with-react-and-themeprovider/
// const lightTheme = {
//   body: '#E2E2E2',
//   text: '#363537',
//   toggleBorder: '#FFF',
//   gradient: 'linear-gradient(#39598A, #79D7ED)',
// }

const darkTheme = createTheme({
  palette: {
    type: 'dark',
  },
});

const URL = "http://ec2-3-95-250-56.compute-1.amazonaws.com:45556/"

const App = () => {
  const [results, setResults] = useState([]);
  const [isLoading, setIsLoading] = useState(false);

  const fetchResult = async query => {
    // setIsLoading(true);
    // setTimeout(() => {
    //   const queryResults = RESULTS.map(item => JSON.parse(item));
    //   setResults(queryResults);
    //   setIsLoading(false);
    // }, 2000);

    setIsLoading(true);
    const response = await fetch(`${URL}/search?` + new URLSearchParams({ query }));
    let queryResults = await response.json();
    queryResults = queryResults.map(item => JSON.parse(item));
    // console.log(queryResults);
    setResults(queryResults);
    setIsLoading(false);
  };

  return (
    <ThemeProvider theme={darkTheme}>
      <Search handleSearch={fetchResult} isShowingResults={results.length > 0 || isLoading} />
      <Results results={results} isLoading={isLoading} />
    </ThemeProvider>
  );
};

export default App;