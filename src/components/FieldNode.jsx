import { Handle, Position } from 'reactflow';
import { FaArrowCircleRight, FaArrowCircleLeft } from 'react-icons/fa';
 
export default function FieldNode({ fields }) {
  return (
    <>
<div style={{ padding: 10, border: '1px solid black', borderRadius: 5 }}>
      <table>
        <thead>
          <tr>
            <th>Field Name</th>
            <th>Data Type</th>
          </tr>
        </thead>
        <tbody>
          {/* {fields.map((row, index) => (  */}
            {/* <tr key={index}> */}
            <tr key='1'>
              <td style={{ position: 'relative' }}>
                <Handle
                  type="source"
                  position="left"
                  // id={`source-${index}`}
                  id='source-1'
                  style={{ top: 10, background: '#555' }}
                >
                <FaArrowCircleLeft style={{ color: '#555', position: 'absolute', left: '-25px', top: '50%', transform: 'translateY(-50%)' }} />
                </Handle>
                <input type='text' className='advancedSearchTextbox'></input>
                {/* {row.source} */}
              </td>
              <td style={{ position: 'relative' }}>
                <Handle
                  type="target"
                  position="right"
                  // id={`target-${index}`}
                  id='target-1'
                  style={{ top: 10, background: '#555' }}
                >
                <FaArrowCircleRight style={{ color: '#555', position: 'absolute', right: '-25px', top: '50%', transform: 'translateY(-50%)' }} />
                </ Handle>
                <input type='text' className='advancedSearchTextbox'></input>
                {/* {row.target} */}
              </td>
            </tr>
          {/* ))} */}
        </tbody>
      </table>
    </div>
    </>
  );
};