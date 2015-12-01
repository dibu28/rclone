package src

import (
	"fmt"
	"io/ioutil"
	"net/http"
)

// PerformMkdir does the actual mkdir via PUT request.
func (c *Client) PerformMkdir(url string) (error, int) {
	req, err := http.NewRequest("PUT", url, nil)
	if err != nil {
		return err, 0
	}

	//set access token and headers
	c.setRequestScope(req)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err, 0
	}

	if resp.StatusCode != 201 {
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return err, 0
		}
		return fmt.Errorf("Create Folder error [%d]: %s", resp.StatusCode, string(body[:])), resp.StatusCode
	}
	return nil, resp.StatusCode
}
