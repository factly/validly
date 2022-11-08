from pydantic import BaseModel


class AllExpectationDocs(BaseModel):
    state_expectation: str = """
<h1 class="sub-title is-size-6">State names should standardised these can be reference from data dictionary</h1>
<strong>Standard State names</strong> :
<div class="tags are-small">
    <span class="tag has-background-primary-light is-rounded is-responsive is-bordered has-text-weight-bold">Andaman and Nicobar Islands</span>
    <span class="tag has-background-primary-light is-rounded is-responsive is-bordered has-text-weight-bold">Andhra Pradesh</span>
    <span class="tag has-background-primary-light is-rounded is-responsive is-bordered has-text-weight-bold">Arunachal Pradesh</span>
    <span class="tag has-background-primary-light is-rounded is-responsive is-bordered has-text-weight-bold">Assam</span>
    <span class="tag has-background-primary-light is-rounded is-responsive is-bordered has-text-weight-bold">Bihar</span>
    <span class="tag has-background-primary-light is-rounded is-responsive is-bordered has-text-weight-bold">Chandigarh</span>
    <span class="tag has-background-primary-light is-rounded is-responsive is-bordered has-text-weight-bold">Chhattisgarh</span>
    <span class="tag has-background-primary-light is-rounded is-responsive is-bordered has-text-weight-bold">Dadra and Nagar Haveli</span>
    <span class="tag has-background-primary-light is-rounded is-responsive is-bordered has-text-weight-bold">Daman and Diu</span>
    <span class="tag has-background-primary-light is-rounded is-responsive is-bordered has-text-weight-bold">Delhi</span>
    <span class="tag has-background-primary-light is-rounded is-responsive is-bordered has-text-weight-bold">Goa</span>
    <span class="tag has-background-primary-light is-rounded is-responsive is-bordered has-text-weight-bold">Gujarat</span>
    <span class="tag has-background-primary-light is-rounded is-responsive is-bordered has-text-weight-bold">Haryana</span>
    <span class="tag has-background-primary-light is-rounded is-responsive is-bordered has-text-weight-bold">Himachal Pradesh</span>
    <span class="tag has-background-primary-light is-rounded is-responsive is-bordered has-text-weight-bold">Jammu and Kashmir</span>
    <span class="tag has-background-primary-light is-rounded is-responsive is-bordered has-text-weight-bold">Jharkhand</span>
    <span class="tag has-background-primary-light is-rounded is-responsive is-bordered has-text-weight-bold">Karnataka</span>
    <span class="tag has-background-primary-light is-rounded is-responsive is-bordered has-text-weight-bold">Kerala</span>
    <span class="tag has-background-primary-light is-rounded is-responsive is-bordered has-text-weight-bold">Ladakh</span>
    <span class="tag has-background-primary-light is-rounded is-responsive is-bordered has-text-weight-bold">Lakshadweep</span>
    <span class="tag has-background-primary-light is-rounded is-responsive is-bordered has-text-weight-bold">M/O Defence</span>
    <span class="tag has-background-primary-light is-rounded is-responsive is-bordered has-text-weight-bold">M/O Railways</span>
    <span class="tag has-background-primary-light is-rounded is-responsive is-bordered has-text-weight-bold">Madhya Pradesh</span>
    <span class="tag has-background-primary-light is-rounded is-responsive is-bordered has-text-weight-bold">Maharashtra</span>
    <span class="tag has-background-primary-light is-rounded is-responsive is-bordered has-text-weight-bold">Manipur</span>
    <span class="tag has-background-primary-light is-rounded is-responsive is-bordered has-text-weight-bold">Meghalaya</span>
    <span class="tag has-background-primary-light is-rounded is-responsive is-bordered has-text-weight-bold">Mizoram</span>
    <span class="tag has-background-primary-light is-rounded is-responsive is-bordered has-text-weight-bold">Nagaland</span>
    <span class="tag has-background-primary-light is-rounded is-responsive is-bordered has-text-weight-bold">Odisha</span>
    <span class="tag has-background-primary-light is-rounded is-responsive is-bordered has-text-weight-bold">Puducherry</span>
    <span class="tag has-background-primary-light is-rounded is-responsive is-bordered has-text-weight-bold">Punjab</span>
    <span class="tag has-background-primary-light is-rounded is-responsive is-bordered has-text-weight-bold">Rajasthan</span>
    <span class="tag has-background-primary-light is-rounded is-responsive is-bordered has-text-weight-bold">Sikkim</span>
    <span class="tag has-background-primary-light is-rounded is-responsive is-bordered has-text-weight-bold">Tamil Nadu</span>
    <span class="tag has-background-primary-light is-rounded is-responsive is-bordered has-text-weight-bold">Telangana</span>
    <span class="tag has-background-primary-light is-rounded is-responsive is-bordered has-text-weight-bold">Total</span>
    <span class="tag has-background-primary-light is-rounded is-responsive is-bordered has-text-weight-bold">Tripura</span>
    <span class="tag has-background-primary-light is-rounded is-responsive is-bordered has-text-weight-bold">Uttar Pradesh</span>
    <span class="tag has-background-primary-light is-rounded is-responsive is-bordered has-text-weight-bold">Uttarakhand</span>
    <span class="tag has-background-primary-light is-rounded is-responsive is-bordered has-text-weight-bold">West Bengal</span>
</div>
<article>
    <strong>Example</strong> : For the below example <strong>J & K</strong> is the improper naming convention of state, it should be replaced by its proper name <strong>Jammu and Kashmir</strong>
</article>
<div class="columns mt-4">
    <div class="column ml-1">
        <table class="table is-bordered is-narrow is-hoverable is-fullwidth ">
            <thead>
                <tr>
                    <th>fiscal_year</th>
                    <th>state</th>
                    <th>value</th>
                </tr>
            </thead>
            <tbody>
                <tr>
                    <td>2018-19</td>
                    <td class="has-background-danger-light">J & K</td>
                    <td>1000000</td>
                </tr>
                <tr>
                    <td>2019-20</td>
                    <td class="has-background-danger-light">J & K</td>
                    <td>1100000</td>
                </tr>
                <tr>
                    <td>2020-21</td>
                    <td class="has-background-danger-light">J & K</td>
                    <td>1300000</td>
                </tr>
            </tbody>
        </table>
    </div>
    <div class="column mr-1 is-size-6">
        <table class="table is-bordered is-narrow is-hoverable is-fullwidth">
            <thead>
                <tr>
                    <th>fiscal_year</th>
                    <th>state</th>
                    <th>value</th>
                </tr>
            </thead>
            <tbody>
                <tr>
                    <td>2018-19</td>
                    <td class="has-background-success-light">Jammu and Kashmir</td>
                    <td>1000000</td>
                </tr>
                <tr>
                    <td>2019-20</td>
                    <td class="has-background-success-light">Jammu and Kashmir</td>
                    <td>1100000</td>

                </tr>
                <tr>
                    <td>2020-21</td>
                    <td class="has-background-success-light">Jammu and Kashmir</td>
                    <td>1300000</td>
                </tr>
            </tbody>
        </table>
    </div>
</div>
    """

    special_character_expectation: str = """
<h1 class="sub-title is-size-6">Values inside Table should not have special Character</h1>
<article>
    <strong>Special Charaters</strong> : Everythings <strong>excepts</strong> <i>A-Z, a-z, 0-9, "_" , "."</i>
</article>
<article>
    <strong>Example</strong> : For the below example <strong>**</strong> is the special character, it has some significance which is generally provided from the source. Thus this special character is not allowed in the values but must be preserve as additional
    information inside Notes.
</article>
<div class="columns mt-4">
    <div class="column ml-1 is-two-fifths">
        <table class="table is-bordered is-narrow is-hoverable is-fullwidth ">
            <thead>
                <tr>
                    <th>fiscal_year</th>
                    <th>state</th>
                    <th>value</th>
                </tr>
            </thead>
            <tfoot>
                <tr>
                    <td colspan="3">** Means values for that 2020-21 are provisional</td>


                </tr>
            </tfoot>
            <tbody>
                <tr>
                    <td>2018-19</td>
                    <td>Andhra Pradesh</td>
                    <td>1000000</td>
                </tr>
                <tr>
                    <td>2019-20</td>
                    <td>Andhra Pradesh</td>
                    <td>1100000</td>
                </tr>
                <tr class="has-background-danger-light">
                    <td>2020-21**</td>
                    <td>Andhra Pradesh</td>
                    <td>1300000</td>
                </tr>
            </tbody>
        </table>
    </div>
    <div class="column mr-1 is-size-6">
        <table class="table is-bordered is-narrow is-hoverable is-fullwidth">
            <thead>
                <tr>
                    <th>fiscal_year</th>
                    <th>state</th>
                    <th>value</th>
                    <th>note</th>
                </tr>
            </thead>
            <tbody>
                <tr>
                    <td>2018-19</td>
                    <td>Andhra Pradesh</td>
                    <td>1000000</td>
                    <td></td>
                </tr>
                <tr>
                    <td>2019-20</td>
                    <td>Andhra Pradesh</td>
                    <td>1100000</td>
                    <td></td>

                </tr>
                <tr class="has-background-success-light">
                    <td>2020-21</td>
                    <td>Andhra Pradesh</td>
                    <td>1300000</td>
                    <td>fiscal_year : Provisional value</td>
                </tr>


            </tbody>
        </table>
    </div>
</div>
    """
